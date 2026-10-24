# Databricks notebook source
# MAGIC %md
# MAGIC # Avaliação · esperado × encontrado (CRISP-DM fase 5)
# MAGIC
# MAGIC Confere, a cada execução do pipeline, se a silver e a gold reproduzem os números de referência confirmados na fase 2
# MAGIC (`02_entendimento_dados`) e se as regras de qualidade trataram o volume esperado. Grava o resultado em
# MAGIC `gold.avaliacao` (histórico) e **falha o job** se algum item divergir: número errado não segue para o dashboard.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
CATALOG = dbutils.widgets.get("catalog")
S, G = f"{CATALOG}.silver", f"{CATALOG}.gold"
EXECUCAO = datetime.now(timezone.utc)


def valor(sql):
    return str(spark.sql(sql).first()[0])


def regra(tabela, regra_, tratamento):
    """Ocorrências de uma regra na última execução da silver."""
    return valor(f"""select coalesce(sum(ocorrencias), 0) from {S}.qualidade
                     where tabela = '{tabela}' and regra = '{regra_}' and tratamento = '{tratamento}'
                       and _processado_em = (select max(_processado_em) from {S}.qualidade where tabela = '{tabela}')""")


# (dor ou requisito, item, esperado, encontrado)
checagens = [
    ("D1", "projetos em Excel · ClickUp · MS Project", "20 · 25 · 15",
     valor(f"""select concat_ws(' · ', count(distinct projeto_id) filter (where ferramenta = 'excel'),
                                      count(distinct projeto_id) filter (where ferramenta = 'clickup'),
                                      count(distinct projeto_id) filter (where ferramenta = 'msproject'))
               from {S}.tarefas""")),
    ("D1", "tarefas na silver (1.663 − 20 + 2.119 + 1.260 − 105)", "4917", valor(f"select count(*) from {S}.tarefas")),
    ("D1", "projetos em andamento", "41", valor(f"select projetos_em_andamento from {G}.kpi_carteira")),
    ("D1", "SPI do PRJ-011 × 100", "77", valor(f"select cast(round(spi * 100) as int) from {G}.kpi_projeto where projeto_id = 'PRJ-011'")),
    ("D1", "projetos críticos (SPI ou CPI < 0,8)", "7", valor(f"select projetos_criticos from {G}.kpi_carteira")),
    ("D1", "projetos em atenção (< 0,9, sem crítico)", "10", valor(f"select projetos_em_atencao from {G}.kpi_carteira")),
    ("D2", "custo real total (R$ mi)", "193.35", valor(f"select round(sum(valor) / 1e6, 2) from {S}.custos")),
    ("D2", "estouro previsto da carteira (R$ mi)", "6.47", valor(f"select round(estouro_previsto_total / 1e6, 2) from {G}.kpi_carteira")),
    ("D2", "maior estouro", "PRJ-021", valor(f"select maior_estouro_projeto from {G}.kpi_carteira")),
    ("D3", "melancias (farol verde e SPI < 0,9)", "PRJ-012, PRJ-028, PRJ-034, PRJ-042",
     valor(f"select array_join(array_sort(collect_list(projeto_id)), ', ') from {G}.kpi_projeto where melancia")),
    ("RNF", "Q5 custos com vírgula corrigidos", "671", regra("custos", "Q5", "corrigido")),
    ("RNF", "Q7 linhas TOTAL descartadas", "20", regra("tarefas", "Q7", "descartado")),
    ("RNF", "Q11 tarefas-resumo descartadas", "105", regra("tarefas", "Q11", "descartado")),
    ("RNF", "Q3 datas dd/MM/aaaa convertidas", "24", regra("projetos", "Q3", "corrigido")),
    ("RNF", "linhas em quarentena (não corrigíveis)", "0",
     valor(f"select count(*) from {S}.quarentena where tratamento = 'quarentena'")),
]

resultado = (
    spark.createDataFrame(checagens, "escopo string, item string, esperado string, encontrado string")
    .withColumn("confere", F.col("esperado") == F.col("encontrado"))
    .withColumn("_executado_em", F.lit(EXECUCAO).cast("timestamp"))
)
resultado.write.mode("append").saveAsTable(f"{G}.avaliacao")
resultado.drop("_executado_em").show(truncate=False)

divergentes = resultado.filter("not confere").count()
assert divergentes == 0, f"{divergentes} item(ns) divergente(s) do esperado; ver {G}.avaliacao"
print(f"Avaliação OK: {len(checagens)} de {len(checagens)} itens conferem")
