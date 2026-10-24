# Databricks notebook source
# MAGIC %md
# MAGIC # Silver · cadastros, status semanal e custos (CRISP-DM fase 3: preparação dos dados)
# MAGIC
# MAGIC Transforma a **versão vigente** de três tabelas da bronze em tabelas limpas e tipadas na silver. Cada tabela é
# MAGIC declarada como uma `Preparacao` (origem, transformação e regras de qualidade); um único fluxo executa todas.
# MAGIC
# MAGIC | Etapa | O que faz |
# MAGIC |---|---|
# MAGIC | 1. Configuração | catálogo, schemas e identificação da execução |
# MAGIC | 2. Regras e preparações | declara as regras Q1–Q6 e Q13 e as 3 preparações (`projetos`, `status_semanal`, `custos`) |
# MAGIC | 3. Leitura | versão vigente de cada registro na bronze (extração mais recente por `_chave`) |
# MAGIC | 4. Transformação | tipos, formatos e normalizações de cada tabela |
# MAGIC | 5. Qualidade | conta as linhas corrigidas e sinalizadas por regra; o que viola uma regra de quarentena sai da tabela |
# MAGIC | 6. Gravação | sobrescreve a tabela da silver (recálculo idempotente) |
# MAGIC | 7. Registro | grava `silver.qualidade` (histórico) e mostra o resumo |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
CATALOG = dbutils.widgets.get("catalog")
B, S = f"{CATALOG}.bronze", f"{CATALOG}.silver"
EXECUCAO = datetime.now(timezone.utc)
LOTE = uuid.uuid4().hex[:12]
spark.sql(f"create schema if not exists {S}")
spark.sql(f"""create table if not exists {S}.quarentena
              (tabela string, chave string, motivo string, tratamento string, registro string,
               _processado_em timestamp, _lote string)""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Regras e preparações
# MAGIC
# MAGIC Uma `Regra` é uma expressão SQL que fica verdadeira na linha afetada. O `tratamento` diz o que acontece:
# MAGIC `corrigido` e `sinalizado` só contam as linhas (a correção já está na transformação); `quarentena` tira a linha da
# MAGIC tabela e guarda em `silver.quarentena` com o motivo.

# COMMAND ----------


@dataclass(frozen=True)
class Regra:
    codigo: str
    descricao: str
    tratamento: str  # corrigido | sinalizado | quarentena
    expressao: str   # SQL booleana: verdadeira na linha afetada


@dataclass(frozen=True)
class Preparacao:
    tabela: str                                  # destino na silver
    origem: str                                  # tabela da bronze
    transformar: Callable[[DataFrame], DataFrame]
    regras: list = field(default_factory=list)
    colunas_auxiliares: tuple = ()               # usadas só pelas regras; não vão para a silver

    @property
    def alvo(self) -> str:
        return f"{S}.{self.tabela}"


def data_dois_formatos(coluna: str):
    """'14/03/2025' ou '2025-03-14' -> date; formato inválido vira nulo (e cai na quarentena)."""
    c = F.col(coluna)
    return (F.when(c.contains("/"), F.to_date(F.try_to_timestamp(c, F.lit("dd/MM/yyyy"))))
            .otherwise(F.to_date(F.try_to_timestamp(c, F.lit("yyyy-MM-dd")))))


def quarentena(codigo: str, expressao: str) -> Regra:
    return Regra(codigo, f"violação: {expressao}", "quarentena", expressao)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Projetos (Q1, Q2, Q3)

# COMMAND ----------

FERRAMENTA = {"excel": "excel", "planilha": "excel", "clickup": "clickup",
              "ms project": "msproject", "project": "msproject", "msproject": "msproject"}


def transformar_projetos(df: DataFrame) -> DataFrame:
    mapa = F.create_map(*[F.lit(x) for kv in FERRAMENTA.items() for x in kv])
    return df.select(
        "_chave", "projeto_id", "nome_projeto", "cliente", "segmento", "tipo_projeto",
        F.col("ferramenta").alias("ferramenta_original"),
        mapa[F.lower(F.trim("ferramenta"))].alias("ferramenta"),
        F.nullif(F.trim("codigo_ferramenta"), F.lit("")).alias("codigo_ferramenta"),
        F.expr("try_cast(valor_contrato as decimal(18,2))").alias("valor_contrato"),
        F.expr("try_cast(orcamento_custo as decimal(18,2))").alias("orcamento_custo"),
        data_dois_formatos("data_inicio").alias("data_inicio"),
        F.col("data_inicio").contains("/").alias("_data_inicio_com_barra"),
        data_dois_formatos("data_fim_baseline").alias("data_fim_baseline"),
        F.lower(F.trim("status")).alias("status"),
        F.col("gerente_projeto").alias("gerente"),
    ).withColumn("sem_codigo_ferramenta", F.col("codigo_ferramenta").isNull())


PROJETOS = Preparacao(
    tabela="projetos",
    origem="pg_pmo_projetos",
    transformar=transformar_projetos,
    regras=[
        Regra("Q1", "ferramenta normalizada para excel/clickup/msproject", "corrigido",
              "ferramenta_original not in ('Excel', 'ClickUp', 'MS Project')"),
        Regra("Q2", "projeto sem código da ferramenta (casado pelo nome ou pelo arquivo)", "sinalizado",
              "codigo_ferramenta is null"),
        Regra("Q3", "data_inicio em dd/MM/aaaa convertida", "corrigido", "_data_inicio_com_barra"),
        quarentena("Q1 ferramenta desconhecida", "ferramenta is null"),
        quarentena("Q3 data inválida", "data_inicio is null or data_fim_baseline is null"),
        quarentena("valor não numérico", "valor_contrato is null or orcamento_custo is null"),
    ],
    colunas_auxiliares=("ferramenta_original", "_data_inicio_com_barra"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Status semanal (Q4, Q13)

# COMMAND ----------


def transformar_status(df: DataFrame) -> DataFrame:
    return df.select(
        "_chave", "projeto_id",
        F.to_date(F.try_to_timestamp("data_status", F.lit("yyyy-MM-dd"))).alias("data_status"),
        "gerente",
        F.col("farol").alias("_farol_original"),
        F.lower(F.trim("farol")).alias("farol"),
        F.expr("try_cast(pct_concluido as int)").alias("pct_concluido_declarado"),
        F.nullif(F.trim("texto_status"), F.lit("")).alias("texto_status"),
    ).withColumn("texto_vazio", F.col("texto_status").isNull())


STATUS = Preparacao(
    tabela="status_semanal",
    origem="pg_pmo_status_semanal",
    transformar=transformar_status,
    regras=[
        Regra("Q4", "farol com caixa diferente de minúsculas, normalizado", "corrigido", "_farol_original <> farol"),
        Regra("Q13", "texto de status vazio", "sinalizado", "texto_status is null"),
        quarentena("Q4 farol fora do domínio", "farol not in ('verde', 'amarelo', 'vermelho') or farol is null"),
        quarentena("data inválida", "data_status is null"),
    ],
    colunas_auxiliares=("_farol_original",),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Custos (Q5, Q6)

# COMMAND ----------


def transformar_custos(df: DataFrame) -> DataFrame:
    return df.select(
        "_chave", "lancamento_id", "projeto_id",
        F.to_date(F.try_to_timestamp("data", F.lit("yyyy-MM-dd"))).alias("data"),
        F.lower(F.trim("categoria")).alias("categoria"), "descricao", "centro_custo",
        F.col("valor").alias("_valor_original"),
        F.expr("try_cast(replace(valor, ',', '.') as decimal(18,2))").alias("valor"),
    ).withColumn("estorno", F.col("valor") < 0)


CUSTOS = Preparacao(
    tabela="custos",
    origem="pg_financeiro_custos",
    transformar=transformar_custos,
    regras=[
        Regra("Q5", "vírgula decimal trocada por ponto", "corrigido", "_valor_original like '%,%'"),
        Regra("Q6", "lançamento negativo (estorno), mantido", "sinalizado", "valor < 0"),
        quarentena("Q5 valor não numérico", "valor is null"),
        quarentena("data inválida", "data is null"),
    ],
    colunas_auxiliares=("_valor_original",),
)

PREPARACOES = [PROJETOS, STATUS, CUSTOS]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 a 6. Leitura, transformação, qualidade e gravação
# MAGIC
# MAGIC Uma função por etapa; `preparar` encadeia as etapas para uma `Preparacao`.

# COMMAND ----------


def ler_vigente(origem: str) -> DataFrame:
    """3. Leitura: a bronze guarda versões; vale a extração mais recente de cada chave natural."""
    return spark.sql(f"""
        select * except (_hash, _origem, _arquivo, _carregado_em, _lote_id, dt_raw)
        from {B}.{origem}
        qualify row_number() over (partition by _chave order by _extraido_em desc, _hash) = 1
    """)


def aplicar_qualidade(df: DataFrame, prep: Preparacao) -> tuple:
    """5. Qualidade: conta cada regra; separa as linhas que violam regras de quarentena."""
    contagens = [(prep.tabela, r.codigo, r.descricao, r.tratamento, df.filter(r.expressao).count()) for r in prep.regras]
    de_quarentena = [r for r in prep.regras if r.tratamento == "quarentena"]
    motivo = F.concat_ws("; ", *[F.when(F.expr(r.expressao), F.lit(r.codigo)) for r in de_quarentena])
    df = df.withColumn("_motivo", F.nullif(motivo, F.lit("")))
    (df.filter("_motivo is not null")
     .select(F.lit(prep.tabela).alias("tabela"), F.col("_chave").alias("chave"), F.col("_motivo").alias("motivo"),
             F.lit("quarentena").alias("tratamento"), F.to_json(F.struct("*")).alias("registro"))
     .withColumn("_processado_em", F.lit(EXECUCAO).cast("timestamp")).withColumn("_lote", F.lit(LOTE))
     .write.mode("append").saveAsTable(f"{S}.quarentena"))
    return df.filter("_motivo is null").drop("_motivo"), contagens


def gravar(df: DataFrame, prep: Preparacao):
    """6. Gravação: tira as colunas auxiliares e sobrescreve a tabela da silver."""
    (df.drop(*prep.colunas_auxiliares)
     .withColumn("_processado_em", F.lit(EXECUCAO).cast("timestamp"))
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(prep.alvo))


def preparar(prep: Preparacao) -> list:
    """Encadeia 3 → 4 → 5 → 6 para uma preparação e devolve as contagens de qualidade."""
    spark.sql(f"delete from {S}.quarentena where tabela = '{prep.tabela}'")
    limpo, contagens = aplicar_qualidade(prep.transformar(ler_vigente(prep.origem)), prep)
    gravar(limpo, prep)
    return contagens


qualidade = [linha for prep in PREPARACOES for linha in preparar(prep)]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Registro
# MAGIC
# MAGIC Uma linha por regra e tabela em `silver.qualidade` (histórico por execução).

# COMMAND ----------

q = (spark.createDataFrame(qualidade, "tabela string, regra string, descricao string, tratamento string, ocorrencias long")
     .withColumn("_processado_em", F.lit(EXECUCAO).cast("timestamp")).withColumn("_lote", F.lit(LOTE)))
q.write.mode("append").saveAsTable(f"{S}.qualidade")
q.drop("_processado_em", "_lote").show(truncate=False)
for prep in PREPARACOES:
    print(prep.tabela, spark.table(prep.alvo).count(), "linhas | quarentena:",
          spark.table(f"{S}.quarentena").filter(f"tabela = '{prep.tabela}'").count())
