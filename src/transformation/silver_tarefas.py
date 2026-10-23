# Databricks notebook source
# MAGIC %md
# MAGIC # Silver · modelo único de tarefas (CRISP-DM fase 3: preparação dos dados)
# MAGIC
# MAGIC Integra as tarefas das três ferramentas (Excel, ClickUp, MS Project) num modelo só, `silver.tarefas`, que é a base
# MAGIC do valor agregado (D1, D2) e do farol calculado (D3).
# MAGIC
# MAGIC | Etapa da preparação | O que acontece aqui | Regras (fase 2) |
# MAGIC |---|---|---|
# MAGIC | Limpeza | linha `TOTAL` do Excel e tarefas-resumo do MS Project descartadas, com registro | Q7, Q11 |
# MAGIC | Padronização de tipos | datas (texto, número de série, milissegundos, ISO), % (símbolo, fração, número) e horas (`h`, ms, `PT..H`) num formato só | Q8, Q9, Q10 |
# MAGIC | Wrangling | tarefas do ClickUp e do Project abertas a partir do JSON; fase do Project herdada da tarefa-resumo | — |
# MAGIC | Integração | as três ferramentas empilhadas e ligadas ao cadastro de projetos (ClickUp pelo código ou pelo nome) | Q2, Q12 |
# MAGIC | Engenharia de atributos | fração planejada até a data de corte, horas agregadas e planejadas, desvio e atraso por tarefa | — |
# MAGIC
# MAGIC Depende de `silver_cadastros` (projetos e data de corte).

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
qualidade = []


def registrar(regra, descricao, tratamento, ocorrencias):
    qualidade.append(("tarefas", regra, descricao, tratamento, int(ocorrencias)))


def para_quarentena(df: DataFrame, motivo: str, tratamento: str):
    (df.select(F.lit("tarefas").alias("tabela"), F.col("tarefa_id").alias("chave"), F.lit(motivo).alias("motivo"),
               F.lit(tratamento).alias("tratamento"), F.to_json(F.struct("*")).alias("registro"))
     .withColumn("_processado_em", F.lit(EXECUCAO).cast("timestamp")).withColumn("_lote", F.lit(LOTE))
     .write.mode("append").saveAsTable(f"{S}.quarentena"))


for tabela in ["mongo_excel_cronogramas", "mongo_clickup_listas", "mongo_msproject_projetos"]:
    spark.sql(f"""
        select * from {B}.{tabela}
        qualify row_number() over (partition by _chave order by _extraido_em desc, _hash) = 1
    """).createOrReplaceTempView(tabela)
spark.table(f"{S}.projetos").createOrReplaceTempView("projetos")
spark.sql(f"delete from {S}.quarentena where tabela = 'tarefas'")
DATA_CORTE = spark.sql(f"select max(data_status) from {S}.status_semanal").first()[0]
print("Data de corte:", DATA_CORTE)

# Conversões reutilizadas (SQL)
DATA_EXCEL = """case when {c} like '%/%' then to_date(try_to_timestamp({c}, 'dd/MM/yyyy'))
                     else date_add(date'1899-12-30', try_cast(try_cast({c} as double) as int)) end"""
DATA_MS = "to_date(timestamp_millis(try_cast({c} as bigint)))"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cada ferramenta no formato comum

# COMMAND ----------

excel = spark.sql(f"""
    with e as (
        select _chave, regexp_extract(_chave, 'PRJ-[0-9]{{3}}', 0) as projeto_id,
               from_json(documento, 'map<string,string>') as m
        from mongo_excel_cronogramas
    )
    select _chave as tarefa_id, projeto_id, 'excel' as ferramenta,
           m['Tarefa'] as tarefa, lower(m['Fase']) as fase, m['Responsável'] as responsavel,
           cast(null as string) as status_tarefa,
           {DATA_EXCEL.format(c="m['Início previsto']")} as inicio_baseline,
           {DATA_EXCEL.format(c="m['Término previsto']")} as fim_baseline,
           {DATA_EXCEL.format(c="m['Início real']")} as inicio_real,
           {DATA_EXCEL.format(c="m['Término real']")} as fim_real,
           case when m['% Concluído'] like '%\\%%'
                then try_cast(replace(replace(m['% Concluído'], '%', ''), ',', '.') as double)
                else try_cast(m['% Concluído'] as double) * 100 end as pct_bruto,
           try_cast(replace(m['Horas previstas'], 'h', '') as double) as horas_previstas,
           -- marcadores para o registro de qualidade
           m['Início previsto'] not like '%/%' or m['Término previsto'] not like '%/%' as _data_serial,
           m['% Concluído'] not like '%\\%%' as _pct_fracao,
           m['Horas previstas'] like '%h' as _horas_texto
    from e
""")
total = excel.filter("tarefa = 'TOTAL'")
registrar("Q7", "linha TOTAL do Excel descartada", "descartado", total.count())
para_quarentena(total, "Q7 linha TOTAL (soma das tarefas)", "descartado")
excel = excel.filter("tarefa <> 'TOTAL'")
registrar("Q8", "Excel: data de baseline em número de série convertida", "corrigido", excel.filter("_data_serial").count())
registrar("Q9", "Excel: % em fração convertido para 0–100", "corrigido", excel.filter("_pct_fracao").count())
registrar("Q10", "Excel: horas com sufixo h convertidas", "corrigido", excel.filter("_horas_texto").count())
excel = excel.drop("_data_serial", "_pct_fracao", "_horas_texto")

clickup = spark.sql(f"""
    with t as (
        select get_json_object(documento, '$.list.id') as list_id, get_json_object(documento, '$.list.name') as list_name, x.*
        from mongo_clickup_listas
        lateral view explode(from_json(get_json_object(documento, '$.tasks'),
            'array<struct<id:string,name:string,status:struct<status:string,type:string>,start_date:string,
                          date_done:string,time_estimate:bigint,assignees:array<struct<username:string>>,
                          custom_fields:array<struct<name:string,type:string,value:string>>>>')) e as x
    )
    select t.id as tarefa_id, p.projeto_id, 'clickup' as ferramenta,
           t.name as tarefa, lower(filter(t.custom_fields, f -> f.name = 'Fase')[0].value) as fase,
           t.assignees[0].username as responsavel,
           t.status.status as _status_original, lower(trim(t.status.status)) as status_tarefa,
           {DATA_MS.format(c="filter(t.custom_fields, f -> f.name = 'Início baseline')[0].value")} as inicio_baseline,
           {DATA_MS.format(c="filter(t.custom_fields, f -> f.name = 'Fim baseline')[0].value")} as fim_baseline,
           {DATA_MS.format(c="t.start_date")} as inicio_real,
           {DATA_MS.format(c="t.date_done")} as fim_real,
           try_cast(filter(t.custom_fields, f -> f.name = 'Progresso (%)')[0].value as double) as pct_bruto,
           t.time_estimate / 3600000 as horas_previstas,
           t.list_id
    from t
    left join projetos p on p.codigo_ferramenta = t.list_id or p.nome_projeto = t.list_name
""")
registrar("Q10", "ClickUp: horas em milissegundos convertidas", "corrigido", clickup.count())
registrar("Q12", "ClickUp: status de tarefa normalizado (caixa e espaços)", "corrigido",
          clickup.filter("_status_original <> status_tarefa").count())
sem_projeto = clickup.filter("projeto_id is null")
registrar("Q2", "ClickUp: lista sem projeto no cadastro", "quarentena", sem_projeto.count())
para_quarentena(sem_projeto, "Q2 lista do ClickUp sem projeto no cadastro", "quarentena")
clickup = clickup.filter("projeto_id is not null").drop("_status_original", "list_id")

msproject = spark.sql(f"""
    with t as (
        select _chave as arquivo, regexp_extract(_chave, 'PRJ-[0-9]{{3}}', 0) as projeto_id, x.*
        from mongo_msproject_projetos
        lateral view explode(from_json(get_json_object(documento, '$.projeto.Tasks.Task'),
            'array<struct<UID:string,ID:string,Name:string,OutlineLevel:string,Summary:string,PercentComplete:string,
                          Work:string,ActualStart:string,ActualFinish:string,
                          Baseline:struct<Start:string,Finish:string>>>')) e as x
    )
    select concat(arquivo, '|', UID) as tarefa_id, projeto_id, 'msproject' as ferramenta,
           Name as tarefa,
           -- wrangling: a fase é a tarefa-resumo de nível 1 mais recente acima da tarefa
           lower(last(case when OutlineLevel = '1' then Name end, true)
                 over (partition by arquivo order by try_cast(ID as int) rows unbounded preceding)) as fase,
           cast(null as string) as responsavel, cast(null as string) as status_tarefa,
           to_date(Baseline.Start) as inicio_baseline, to_date(Baseline.Finish) as fim_baseline,
           to_date(ActualStart) as inicio_real, to_date(ActualFinish) as fim_real,
           try_cast(PercentComplete as double) as pct_bruto,
           try_cast(regexp_extract(Work, 'PT([0-9]+)H', 1) as double) as horas_previstas,
           Summary = '1' as _resumo
    from t
""")
resumo = msproject.filter("_resumo")
registrar("Q11", "MS Project: tarefa-resumo descartada (soma as de baixo)", "descartado", resumo.count())
para_quarentena(resumo.drop("_resumo"), "Q11 tarefa-resumo (soma das tarefas de baixo)", "descartado")
msproject = msproject.filter("not _resumo").drop("_resumo")
registrar("Q10", "MS Project: horas em PT..H convertidas", "corrigido", msproject.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Integração, validação e engenharia de atributos

# COMMAND ----------

tarefas = excel.unionByName(clickup).unionByName(msproject)

registrar("Q9", "% concluído acima de 100 limitado a 100", "corrigido", tarefas.filter("pct_bruto > 100").count())
tarefas = (tarefas.withColumn("pct_concluido", F.least(F.lit(100.0), F.col("pct_bruto")))
           .withColumn("pct_acima_de_100_na_origem", F.col("pct_bruto") > 100).drop("pct_bruto"))

violacoes = {
    "projeto inexistente no cadastro": "projeto_id not in (select projeto_id from projetos)",
    "Q8 data de baseline inválida": "inicio_baseline is null or fim_baseline is null",
    "baseline com fim antes do início": "fim_baseline < inicio_baseline",
    "Q9 % concluído inválido": "pct_concluido is null or pct_concluido < 0",
    "Q10 horas inválidas": "horas_previstas is null or horas_previstas <= 0",
}
tarefas.createOrReplaceTempView("tarefas_integradas")
motivo = "concat_ws('; ', " + ", ".join(f"case when {e} then '{r}' end" for r, e in violacoes.items()) + ")"
tarefas = spark.sql(f"select *, nullif({motivo}, '') as _motivo from tarefas_integradas")
ruins = tarefas.filter("_motivo is not null")
for r, e in violacoes.items():
    registrar(r.split(" ")[0] if r.startswith("Q") else "integridade", r, "quarentena",
              spark.sql(f"select count(*) from tarefas_integradas where {e}").first()[0])
(ruins.select(F.lit("tarefas").alias("tabela"), F.col("tarefa_id").alias("chave"), F.col("_motivo").alias("motivo"),
              F.lit("quarentena").alias("tratamento"), F.to_json(F.struct("*")).alias("registro"))
 .withColumn("_processado_em", F.lit(EXECUCAO).cast("timestamp")).withColumn("_lote", F.lit(LOTE))
 .write.mode("append").saveAsTable(f"{S}.quarentena"))

# Engenharia de atributos: quanto da tarefa deveria estar pronto na data de corte, e quanto está
fracao = F.expr(f"greatest(0, least(1, try_divide(datediff(date'{DATA_CORTE}', inicio_baseline), "
                f"datediff(fim_baseline, inicio_baseline))))")
silver_tarefas = (
    tarefas.filter("_motivo is null").drop("_motivo")
    .withColumn("data_corte", F.lit(DATA_CORTE).cast("date"))
    .withColumn("duracao_baseline_dias", F.datediff("fim_baseline", "inicio_baseline"))
    .withColumn("fracao_planejada", fracao)
    .withColumn("pct_planejado", F.col("fracao_planejada") * 100)
    .withColumn("horas_agregadas", F.col("horas_previstas") * F.col("pct_concluido") / 100)
    .withColumn("horas_planejadas", F.col("horas_previstas") * F.col("fracao_planejada"))
    .withColumn("desvio_pp", F.col("pct_concluido") - F.col("pct_planejado"))
    .withColumn("atrasada", F.col("pct_concluido") < F.col("pct_planejado"))
    .withColumn("concluida", F.col("pct_concluido") >= 100)
    .withColumn("_processado_em", F.lit(EXECUCAO).cast("timestamp"))
)
silver_tarefas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.tarefas")

# COMMAND ----------

q = (spark.createDataFrame(qualidade, "tabela string, regra string, descricao string, tratamento string, ocorrencias long")
     .withColumn("_processado_em", F.lit(EXECUCAO).cast("timestamp")).withColumn("_lote", F.lit(LOTE)))
q.write.mode("append").saveAsTable(f"{S}.qualidade")
q.drop("_processado_em", "_lote").show(truncate=False)
spark.sql(f"""select ferramenta, count(*) as tarefas, count(distinct projeto_id) as projetos,
                     sum(cast(atrasada as int)) as atrasadas
              from {S}.tarefas group by ferramenta order by ferramenta""").show()
print("quarentena (tarefas):", spark.table(f"{S}.quarentena").filter("tabela = 'tarefas'").groupBy("tratamento").count().collect())
