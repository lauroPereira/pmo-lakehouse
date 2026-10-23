# Databricks notebook source
# MAGIC %md
# MAGIC # Silver · cadastros, status semanal e custos (CRISP-DM fase 3: preparação dos dados)
# MAGIC
# MAGIC Lê a **versão vigente** de cada registro da bronze e entrega tabelas limpas e tipadas em `silver`:
# MAGIC
# MAGIC | Tabela | Preparação aplicada | Regras (fase 2) |
# MAGIC |---|---|---|
# MAGIC | `silver.projetos` | ferramenta normalizada, datas nos dois formatos, valores numéricos | Q1, Q2, Q3 |
# MAGIC | `silver.status_semanal` | farol normalizado e validado, data e % tipados, texto vazio sinalizado | Q4, Q13 |
# MAGIC | `silver.custos` | vírgula decimal corrigida, valor tipado, estorno sinalizado | Q5, Q6 |
# MAGIC
# MAGIC O que não pode ser corrigido vai para `silver.quarentena` com o motivo; cada regra registra quantas linhas tratou em
# MAGIC `silver.qualidade`. Recalcula tudo a cada execução (o volume é pequeno), então é idempotente.

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
CATALOG = dbutils.widgets.get("catalog")
B, S = f"{CATALOG}.bronze", f"{CATALOG}.silver"
EXECUCAO = datetime.now(timezone.utc)
LOTE = uuid.uuid4().hex[:12]
spark.sql(f"create schema if not exists {S}")

qualidade = []  # (tabela, regra, descricao, tratamento, ocorrencias)


def vigente(tabela: str) -> DataFrame:
    """Versão vigente: a extração mais recente de cada chave natural."""
    return spark.sql(f"""
        select * except (_hash, _origem, _arquivo, _carregado_em, _lote_id, dt_raw)
        from {B}.{tabela}
        qualify row_number() over (partition by _chave order by _extraido_em desc, _hash) = 1
    """)


def registrar(tabela, regra, descricao, tratamento, ocorrencias):
    qualidade.append((tabela, regra, descricao, tratamento, int(ocorrencias)))


def separar(df: DataFrame, tabela: str, condicoes: dict) -> DataFrame:
    """Manda para a quarentena as linhas que violam alguma condição (regra -> expressão de violação)."""
    motivo = F.concat_ws("; ", *[F.when(F.expr(expr), F.lit(regra)) for regra, expr in condicoes.items()])
    df = df.withColumn("_motivo", F.nullif(motivo, F.lit("")))
    ruins = df.filter("_motivo is not null")
    (ruins.select(
        F.lit(tabela).alias("tabela"), F.col("_chave").alias("chave"), F.col("_motivo").alias("motivo"),
        F.lit("quarentena").alias("tratamento"), F.to_json(F.struct("*")).alias("registro"))
     .withColumn("_processado_em", F.lit(EXECUCAO).cast("timestamp")).withColumn("_lote", F.lit(LOTE))
     .write.mode("append").saveAsTable(f"{S}.quarentena"))
    for regra, expr in condicoes.items():
        registrar(tabela, regra, f"violação: {expr}", "quarentena", df.filter(expr).count())
    return df.filter("_motivo is null").drop("_motivo")


def data_dois_formatos(col):
    """'14/03/2025' ou '2025-03-14' -> date; formato inválido vira nulo (e cai na quarentena)."""
    c = F.col(col)
    return F.when(c.contains("/"), F.to_date(F.try_to_timestamp(c, F.lit("dd/MM/yyyy")))) \
            .otherwise(F.to_date(F.try_to_timestamp(c, F.lit("yyyy-MM-dd"))))


# a quarentena é o estado da última execução destas tabelas
spark.sql(f"""create table if not exists {S}.quarentena
              (tabela string, chave string, motivo string, tratamento string, registro string,
               _processado_em timestamp, _lote string)""")
spark.sql(f"delete from {S}.quarentena where tabela in ('projetos', 'status_semanal', 'custos')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Projetos (Q1, Q2, Q3)

# COMMAND ----------

FERRAMENTA = {"excel": "excel", "planilha": "excel", "clickup": "clickup",
              "ms project": "msproject", "project": "msproject", "msproject": "msproject"}
mapa_ferramenta = F.create_map(*[F.lit(x) for kv in FERRAMENTA.items() for x in kv])

p = vigente("pg_pmo_projetos")
projetos = p.select(
    "_chave", "projeto_id", "nome_projeto", "cliente", "segmento", "tipo_projeto",
    F.col("ferramenta").alias("ferramenta_original"),
    mapa_ferramenta[F.lower(F.trim("ferramenta"))].alias("ferramenta"),
    F.nullif(F.trim("codigo_ferramenta"), F.lit("")).alias("codigo_ferramenta"),
    F.expr("try_cast(valor_contrato as decimal(18,2))").alias("valor_contrato"),
    F.expr("try_cast(orcamento_custo as decimal(18,2))").alias("orcamento_custo"),
    data_dois_formatos("data_inicio").alias("data_inicio"),
    F.col("data_inicio").contains("/").alias("_data_inicio_com_barra"),
    data_dois_formatos("data_fim_baseline").alias("data_fim_baseline"),
    F.lower(F.trim("status")).alias("status"),
    F.col("gerente_projeto").alias("gerente"),
)
registrar("projetos", "Q1", "ferramenta normalizada para excel/clickup/msproject",
          "corrigido", projetos.filter(~F.col("ferramenta_original").isin("Excel", "ClickUp", "MS Project")).count())
registrar("projetos", "Q2", "projeto sem código da ferramenta (casado pelo nome ou pelo arquivo)",
          "sinalizado", projetos.filter("codigo_ferramenta is null").count())
registrar("projetos", "Q3", "data_inicio em dd/MM/aaaa convertida", "corrigido",
          projetos.filter("_data_inicio_com_barra").count())
projetos = separar(projetos, "projetos", {
    "Q1 ferramenta desconhecida": "ferramenta is null",
    "Q3 data inválida": "data_inicio is null or data_fim_baseline is null",
    "valor não numérico": "valor_contrato is null or orcamento_custo is null",
})
(projetos.drop("_data_inicio_com_barra", "ferramenta_original")
 .withColumn("sem_codigo_ferramenta", F.col("codigo_ferramenta").isNull())
 .withColumn("_processado_em", F.lit(EXECUCAO).cast("timestamp"))
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.projetos"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Status semanal (Q4, Q13)

# COMMAND ----------

st = vigente("pg_pmo_status_semanal")
status = st.select(
    "_chave", "projeto_id",
    F.to_date(F.try_to_timestamp("data_status", F.lit("yyyy-MM-dd"))).alias("data_status"),
    F.col("gerente"),
    F.col("farol").alias("_farol_original"),
    F.lower(F.trim("farol")).alias("farol"),
    F.expr("try_cast(pct_concluido as int)").alias("pct_concluido_declarado"),
    F.nullif(F.trim("texto_status"), F.lit("")).alias("texto_status"),
)
registrar("status_semanal", "Q4", "farol com caixa diferente de minúsculas, normalizado", "corrigido",
          status.filter("_farol_original <> farol").count())
registrar("status_semanal", "Q13", "texto de status vazio", "sinalizado", status.filter("texto_status is null").count())
status = separar(status, "status_semanal", {
    "Q4 farol fora do domínio": "farol not in ('verde', 'amarelo', 'vermelho') or farol is null",
    "data inválida": "data_status is null",
})
(status.drop("_farol_original")
 .withColumn("texto_vazio", F.col("texto_status").isNull())
 .withColumn("_processado_em", F.lit(EXECUCAO).cast("timestamp"))
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.status_semanal"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custos (Q5, Q6)

# COMMAND ----------

c = vigente("pg_financeiro_custos")
custos = c.select(
    "_chave", "lancamento_id", "projeto_id",
    F.to_date(F.try_to_timestamp("data", F.lit("yyyy-MM-dd"))).alias("data"),
    F.lower(F.trim("categoria")).alias("categoria"), "descricao", "centro_custo",
    F.col("valor").alias("_valor_original"),
    F.expr("try_cast(replace(valor, ',', '.') as decimal(18,2))").alias("valor"),
)
registrar("custos", "Q5", "vírgula decimal trocada por ponto", "corrigido",
          custos.filter(F.col("_valor_original").contains(",")).count())
registrar("custos", "Q6", "lançamento negativo (estorno), mantido", "sinalizado", custos.filter("valor < 0").count())
custos = separar(custos, "custos", {
    "Q5 valor não numérico": "valor is null",
    "data inválida": "data is null",
})
(custos.drop("_valor_original")
 .withColumn("estorno", F.col("valor") < 0)
 .withColumn("_processado_em", F.lit(EXECUCAO).cast("timestamp"))
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.custos"))

# COMMAND ----------

# Registro de qualidade desta execução (histórico: append)
q = (spark.createDataFrame(qualidade, "tabela string, regra string, descricao string, tratamento string, ocorrencias long")
     .withColumn("_processado_em", F.lit(EXECUCAO).cast("timestamp")).withColumn("_lote", F.lit(LOTE)))
q.write.mode("append").saveAsTable(f"{S}.qualidade")
q.drop("_processado_em", "_lote").show(truncate=False)
for t in ["projetos", "status_semanal", "custos"]:
    print(t, spark.table(f"{S}.{t}").count(), "linhas | quarentena:",
          spark.table(f"{S}.quarentena").filter(f"tabela = '{t}'").count())
