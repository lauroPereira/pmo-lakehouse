# Databricks notebook source
# MAGIC %md
# MAGIC # Avaliação · esperado × encontrado (CRISP-DM fase 5)
# MAGIC
# MAGIC Confere, a cada execução do pipeline, se a silver e a gold reproduzem os números de referência confirmados na fase 2
# MAGIC (`02_entendimento_dados`) e se as regras de qualidade trataram o volume esperado. Cada conferência é declarada como uma
# MAGIC `Checagem` (escopo, item, valor esperado e a consulta que mede o valor encontrado).
# MAGIC
# MAGIC | Etapa | O que faz |
# MAGIC |---|---|
# MAGIC | 1. Configuração | catálogo e identificação da execução |
# MAGIC | 2. Checagens | declara as 15 conferências: D1, D2, D3 e o requisito não funcional (RNF) de qualidade |
# MAGIC | 3. Medição | executa a consulta de cada checagem |
# MAGIC | 4. Registro | grava o resultado em `gold.avaliacao` (histórico) |
# MAGIC | 5. Decisão | **falha o job** se algum item divergir: número errado não segue para o dashboard |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
CATALOG = dbutils.widgets.get("catalog")
S, G = f"{CATALOG}.silver", f"{CATALOG}.gold"
EXECUCAO = datetime.now(timezone.utc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Checagens
# MAGIC
# MAGIC `esperado` é o valor de referência; `consulta` é um SQL que devolve um único valor, comparado como texto.

# COMMAND ----------


@dataclass(frozen=True)
class Checagem:
    escopo: str     # D1 | D2 | D3 | RNF
    item: str
    esperado: str
    consulta: str   # SQL que devolve um único valor


def ocorrencias(tabela: str, regra: str, tratamento: str) -> str:
    """SQL das ocorrências de uma regra de qualidade na última execução da silver."""
    return f"""select coalesce(sum(ocorrencias), 0) from {S}.qualidade
               where tabela = '{tabela}' and regra = '{regra}' and tratamento = '{tratamento}'
                 and _processado_em = (select max(_processado_em) from {S}.qualidade where tabela = '{tabela}')"""


CHECAGENS = [
    Checagem("D1", "projetos em Excel · ClickUp · MS Project", "20 · 25 · 15", f"""
        select concat_ws(' · ', count(distinct projeto_id) filter (where ferramenta = 'excel'),
                                count(distinct projeto_id) filter (where ferramenta = 'clickup'),
                                count(distinct projeto_id) filter (where ferramenta = 'msproject'))
        from {S}.tarefas"""),
    Checagem("D1", "tarefas na silver (1.663 − 20 + 2.119 + 1.260 − 105)", "4917", f"select count(*) from {S}.tarefas"),
    Checagem("D1", "projetos em andamento", "41", f"select projetos_em_andamento from {G}.kpi_carteira"),
    Checagem("D1", "SPI do PRJ-011 × 100", "77",
             f"select cast(round(spi * 100) as int) from {G}.kpi_projeto where projeto_id = 'PRJ-011'"),
    Checagem("D1", "projetos críticos (SPI ou CPI < 0,8)", "7", f"select projetos_criticos from {G}.kpi_carteira"),
    Checagem("D1", "projetos em atenção (< 0,9, sem crítico)", "10", f"select projetos_em_atencao from {G}.kpi_carteira"),
    Checagem("D2", "custo real total (R$ mi)", "193.35", f"select round(sum(valor) / 1e6, 2) from {S}.custos"),
    Checagem("D2", "estouro previsto da carteira (R$ mi)", "6.47",
             f"select round(estouro_previsto_total / 1e6, 2) from {G}.kpi_carteira"),
    Checagem("D2", "maior estouro", "PRJ-021", f"select maior_estouro_projeto from {G}.kpi_carteira"),
    Checagem("D3", "melancias (farol verde e SPI < 0,9)", "PRJ-012, PRJ-028, PRJ-034, PRJ-042",
             f"select array_join(array_sort(collect_list(projeto_id)), ', ') from {G}.kpi_projeto where melancia"),
    Checagem("RNF", "Q5 custos com vírgula corrigidos", "671", ocorrencias("custos", "Q5", "corrigido")),
    Checagem("RNF", "Q7 linhas TOTAL descartadas", "20", ocorrencias("tarefas", "Q7", "descartado")),
    Checagem("RNF", "Q11 tarefas-resumo descartadas", "105", ocorrencias("tarefas", "Q11", "descartado")),
    Checagem("RNF", "Q3 datas dd/MM/aaaa convertidas", "24", ocorrencias("projetos", "Q3", "corrigido")),
    Checagem("RNF", "linhas em quarentena (não corrigíveis)", "0",
             f"select count(*) from {S}.quarentena where tratamento = 'quarentena'"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 a 5. Medição, registro e decisão

# COMMAND ----------


def medir(checagem: Checagem) -> tuple:
    """3. Medição: executa a consulta e devolve (escopo, item, esperado, encontrado)."""
    encontrado = str(spark.sql(checagem.consulta).first()[0])
    return (checagem.escopo, checagem.item, checagem.esperado, encontrado)


def registrar(medicoes: list):
    """4. Registro: acrescenta o resultado desta execução em `gold.avaliacao`."""
    resultado = (
        spark.createDataFrame(medicoes, "escopo string, item string, esperado string, encontrado string")
        .withColumn("confere", F.col("esperado") == F.col("encontrado"))
        .withColumn("_executado_em", F.lit(EXECUCAO).cast("timestamp"))
    )
    resultado.write.mode("append").saveAsTable(f"{G}.avaliacao")
    resultado.drop("_executado_em").show(truncate=False)
    return resultado


def decidir(resultado):
    """5. Decisão: qualquer divergência falha o notebook (e o job)."""
    divergentes = resultado.filter("not confere").count()
    assert divergentes == 0, f"{divergentes} item(ns) divergente(s) do esperado; ver {G}.avaliacao"
    print(f"Avaliação OK: {len(CHECAGENS)} de {len(CHECAGENS)} itens conferem")


decidir(registrar([medir(c) for c in CHECAGENS]))
