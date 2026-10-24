# Databricks notebook source
# MAGIC %md
# MAGIC # Silver · modelo único de tarefas (CRISP-DM fase 3: preparação dos dados)
# MAGIC
# MAGIC Integra as tarefas das três ferramentas (Excel, ClickUp, MS Project) num modelo só, `silver.tarefas`, base do valor
# MAGIC agregado (D1, D2) e do farol calculado (D3). Cada ferramenta é declarada como uma `Ferramenta`; um único fluxo
# MAGIC padroniza as três, integra e calcula os atributos.
# MAGIC
# MAGIC | Etapa | O que faz | Regras (fase 2) |
# MAGIC |---|---|---|
# MAGIC | 1. Configuração | catálogo, data de corte, conversões reutilizadas | — |
# MAGIC | 2. Ferramentas | declara, por ferramenta: consulta de padronização, descartes e marcadores de qualidade | — |
# MAGIC | 3. Padronização | wrangling do JSON e tipos (datas, %, horas) num formato comum | Q8, Q9, Q10, Q12 |
# MAGIC | 4. Limpeza | descarta linha `TOTAL` e tarefas-resumo; lista sem projeto vai para quarentena | Q2, Q7, Q11 |
# MAGIC | 5. Integração | empilha as três ferramentas e limita % acima de 100 | Q9 |
# MAGIC | 6. Validação | integridade: projeto existe, datas, %, horas; violação vai para quarentena | — |
# MAGIC | 7. Atributos | fração planejada, horas agregadas e planejadas, desvio e atraso por tarefa | — |
# MAGIC | 8. Gravação e registro | sobrescreve `silver.tarefas` e grava `silver.qualidade` | — |
# MAGIC
# MAGIC Depende de `silver_cadastros` (projetos e data de corte).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
CATALOG = dbutils.widgets.get("catalog")
B, S = f"{CATALOG}.bronze", f"{CATALOG}.silver"
EXECUCAO = datetime.now(timezone.utc)
LOTE = uuid.uuid4().hex[:12]

spark.table(f"{S}.projetos").createOrReplaceTempView("projetos")
DATA_CORTE = spark.sql(f"select max(data_status) from {S}.status_semanal").first()[0]
print("Data de corte:", DATA_CORTE)

DATA_EXCEL = """case when {c} like '%/%' then to_date(try_to_timestamp({c}, 'dd/MM/yyyy'))
                     else date_add(date'1899-12-30', try_cast(try_cast({c} as double) as int)) end"""
DATA_MS = "to_date(timestamp_millis(try_cast({c} as bigint)))"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ferramentas
# MAGIC
# MAGIC Cada `Ferramenta` declara de qual tabela da bronze lê, a consulta que a traz para o formato comum, as `Regra`s de
# MAGIC **descarte** (a linha sai e vai para `silver.quarentena` com o motivo) e os **marcadores** (só contam as linhas que a
# MAGIC padronização corrigiu).

# COMMAND ----------


@dataclass(frozen=True)
class Regra:
    codigo: str
    descricao: str
    tratamento: str   # corrigido | descartado | quarentena
    expressao: str    # SQL booleana: verdadeira na linha afetada
    motivo: str = ""  # texto gravado na quarentena (descartes)


@dataclass(frozen=True)
class Ferramenta:
    nome: str
    origem: str          # tabela da bronze
    padronizar: str      # SQL que lê a view `origem` e devolve o formato comum
    descartes: list = field(default_factory=list)
    marcadores: list = field(default_factory=list)
    colunas_auxiliares: tuple = ()


EXCEL = Ferramenta(
    nome="excel",
    origem="mongo_excel_cronogramas",
    padronizar=f"""
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
               m['Início previsto'] not like '%/%' or m['Término previsto'] not like '%/%' as _data_serial,
               m['% Concluído'] not like '%\\%%' as _pct_fracao,
               m['Horas previstas'] like '%h' as _horas_texto
        from e
    """,
    descartes=[Regra("Q7", "linha TOTAL do Excel descartada", "descartado", "tarefa = 'TOTAL'",
                     "Q7 linha TOTAL (soma das tarefas)")],
    marcadores=[
        Regra("Q8", "Excel: data de baseline em número de série convertida", "corrigido", "_data_serial"),
        Regra("Q9", "Excel: % em fração convertido para 0–100", "corrigido", "_pct_fracao"),
        Regra("Q10", "Excel: horas com sufixo h convertidas", "corrigido", "_horas_texto"),
    ],
    colunas_auxiliares=("_data_serial", "_pct_fracao", "_horas_texto"),
)

CLICKUP = Ferramenta(
    nome="clickup",
    origem="mongo_clickup_listas",
    padronizar=f"""
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
               t.time_estimate / 3600000 as horas_previstas
        from t
        left join projetos p on p.codigo_ferramenta = t.list_id or p.nome_projeto = t.list_name
    """,
    descartes=[Regra("Q2", "ClickUp: lista sem projeto no cadastro", "quarentena", "projeto_id is null",
                     "Q2 lista do ClickUp sem projeto no cadastro")],
    marcadores=[
        Regra("Q10", "ClickUp: horas em milissegundos convertidas", "corrigido", "true"),
        Regra("Q12", "ClickUp: status de tarefa normalizado (caixa e espaços)", "corrigido",
              "_status_original <> status_tarefa"),
    ],
    colunas_auxiliares=("_status_original",),
)

MSPROJECT = Ferramenta(
    nome="msproject",
    origem="mongo_msproject_projetos",
    padronizar="""
        with t as (
            select _chave as arquivo, regexp_extract(_chave, 'PRJ-[0-9]{3}', 0) as projeto_id, x.*
            from mongo_msproject_projetos
            lateral view explode(from_json(get_json_object(documento, '$.projeto.Tasks.Task'),
                'array<struct<UID:string,ID:string,Name:string,OutlineLevel:string,Summary:string,PercentComplete:string,
                              Work:string,ActualStart:string,ActualFinish:string,
                              Baseline:struct<Start:string,Finish:string>>>')) e as x
        )
        select concat(arquivo, '|', UID) as tarefa_id, projeto_id, 'msproject' as ferramenta,
               Name as tarefa,
               lower(last(case when OutlineLevel = '1' then Name end, true)
                     over (partition by arquivo order by try_cast(ID as int) rows unbounded preceding)) as fase,
               cast(null as string) as responsavel, cast(null as string) as status_tarefa,
               to_date(Baseline.Start) as inicio_baseline, to_date(Baseline.Finish) as fim_baseline,
               to_date(ActualStart) as inicio_real, to_date(ActualFinish) as fim_real,
               try_cast(PercentComplete as double) as pct_bruto,
               try_cast(regexp_extract(Work, 'PT([0-9]+)H', 1) as double) as horas_previstas,
               Summary = '1' as _resumo
        from t
    """,
    descartes=[Regra("Q11", "MS Project: tarefa-resumo descartada (soma as de baixo)", "descartado", "_resumo",
                     "Q11 tarefa-resumo (soma das tarefas de baixo)")],
    marcadores=[Regra("Q10", "MS Project: horas em PT..H convertidas", "corrigido", "true")],
    colunas_auxiliares=("_resumo",),
)

FERRAMENTAS = [EXCEL, CLICKUP, MSPROJECT]

# Regras aplicadas depois da integração
LIMITE_PCT = Regra("Q9", "% concluído acima de 100 limitado a 100", "corrigido", "pct_bruto > 100")
INTEGRIDADE = [
    Regra("integridade", "projeto inexistente no cadastro", "quarentena",
          "projeto_id not in (select projeto_id from projetos)"),
    Regra("Q8", "Q8 data de baseline inválida", "quarentena", "inicio_baseline is null or fim_baseline is null"),
    Regra("integridade", "baseline com fim antes do início", "quarentena", "fim_baseline < inicio_baseline"),
    Regra("Q9", "Q9 % concluído inválido", "quarentena", "pct_concluido is null or pct_concluido < 0"),
    Regra("Q10", "Q10 horas inválidas", "quarentena", "horas_previstas is null or horas_previstas <= 0"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 a 8. Do JSON da bronze à `silver.tarefas`

# COMMAND ----------

qualidade = []


def contar(df: DataFrame, regra: Regra) -> int:
    df.createOrReplaceTempView("_avaliada")
    return spark.sql(f"select count(*) from _avaliada where {regra.expressao}").first()[0]


def registrar(regra: Regra, ocorrencias: int):
    qualidade.append(("tarefas", regra.codigo, regra.descricao, regra.tratamento, int(ocorrencias)))


def para_quarentena(df: DataFrame, motivo, tratamento: str):
    """Grava as linhas recebidas em `silver.quarentena` com o motivo (texto ou coluna)."""
    motivo = F.lit(motivo) if isinstance(motivo, str) else motivo
    (df.select(F.lit("tarefas").alias("tabela"), F.col("tarefa_id").alias("chave"), motivo.alias("motivo"),
               F.lit(tratamento).alias("tratamento"), F.to_json(F.struct("*")).alias("registro"))
     .withColumn("_processado_em", F.lit(EXECUCAO).cast("timestamp")).withColumn("_lote", F.lit(LOTE))
     .write.mode("append").saveAsTable(f"{S}.quarentena"))


def ler_vigente(ferramenta: Ferramenta):
    """Versão vigente dos documentos da ferramenta, exposta como view para a consulta de padronização."""
    spark.sql(f"""
        select * from {B}.{ferramenta.origem}
        qualify row_number() over (partition by _chave order by _extraido_em desc, _hash) = 1
    """).createOrReplaceTempView(ferramenta.origem)


def padronizar(ferramenta: Ferramenta) -> DataFrame:
    """3. Padronização: wrangling do JSON e conversão de tipos para o formato comum."""
    ler_vigente(ferramenta)
    return spark.sql(ferramenta.padronizar)


def limpar(df: DataFrame, ferramenta: Ferramenta) -> DataFrame:
    """4. Limpeza: aplica os descartes (com registro na quarentena) e conta os marcadores."""
    for regra in ferramenta.descartes:
        removidas = df.filter(regra.expressao)
        registrar(regra, removidas.count())
        para_quarentena(removidas.drop(*ferramenta.colunas_auxiliares), regra.motivo, regra.tratamento)
        df = df.filter(f"not ({regra.expressao})")
    for regra in ferramenta.marcadores:
        registrar(regra, contar(df, regra))
    return df.drop(*ferramenta.colunas_auxiliares)


def integrar(partes: list) -> DataFrame:
    """5. Integração: empilha as ferramentas e limita o % concluído a 100."""
    df = partes[0]
    for parte in partes[1:]:
        df = df.unionByName(parte)
    registrar(LIMITE_PCT, contar(df, LIMITE_PCT))
    return (df.withColumn("pct_concluido", F.least(F.lit(100.0), F.col("pct_bruto")))
            .withColumn("pct_acima_de_100_na_origem", F.col("pct_bruto") > 100).drop("pct_bruto"))


def validar(df: DataFrame) -> DataFrame:
    """6. Validação: linhas que violam alguma regra de integridade vão para a quarentena."""
    for regra in INTEGRIDADE:
        registrar(regra, contar(df, regra))
    df.createOrReplaceTempView("_integrada")
    motivo = "concat_ws('; ', " + ", ".join(f"case when {r.expressao} then '{r.descricao}' end" for r in INTEGRIDADE) + ")"
    df = spark.sql(f"select *, nullif({motivo}, '') as _motivo from _integrada")
    para_quarentena(df.filter("_motivo is not null"), F.col("_motivo"), "quarentena")
    return df.filter("_motivo is null").drop("_motivo")


def calcular_atributos(df: DataFrame) -> DataFrame:
    """7. Atributos: quanto da tarefa deveria estar pronto na data de corte, e quanto está."""
    fracao = F.expr(f"greatest(0, least(1, try_divide(datediff(date'{DATA_CORTE}', inicio_baseline), "
                    f"datediff(fim_baseline, inicio_baseline))))")
    return (
        df.withColumn("data_corte", F.lit(DATA_CORTE).cast("date"))
        .withColumn("duracao_baseline_dias", F.datediff("fim_baseline", "inicio_baseline"))
        .withColumn("fracao_planejada", fracao)
        .withColumn("pct_planejado", F.col("fracao_planejada") * 100)
        .withColumn("horas_agregadas", F.col("horas_previstas") * F.col("pct_concluido") / 100)
        .withColumn("horas_planejadas", F.col("horas_previstas") * F.col("fracao_planejada"))
        .withColumn("desvio_pp", F.col("pct_concluido") - F.col("pct_planejado"))
        .withColumn("atrasada", F.col("pct_concluido") < F.col("pct_planejado"))
        .withColumn("concluida", F.col("pct_concluido") >= 100)
    )


def gravar(df: DataFrame):
    """8. Gravação: sobrescreve `silver.tarefas` (recálculo idempotente)."""
    (df.withColumn("_processado_em", F.lit(EXECUCAO).cast("timestamp"))
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.tarefas"))


spark.sql(f"delete from {S}.quarentena where tabela = 'tarefas'")
partes = [limpar(padronizar(f), f) for f in FERRAMENTAS]
gravar(calcular_atributos(validar(integrar(partes))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registro

# COMMAND ----------

q = (spark.createDataFrame(qualidade, "tabela string, regra string, descricao string, tratamento string, ocorrencias long")
     .withColumn("_processado_em", F.lit(EXECUCAO).cast("timestamp")).withColumn("_lote", F.lit(LOTE)))
q.write.mode("append").saveAsTable(f"{S}.qualidade")
q.drop("_processado_em", "_lote").show(truncate=False)
spark.sql(f"""select ferramenta, count(*) as tarefas, count(distinct projeto_id) as projetos,
                     sum(cast(atrasada as int)) as atrasadas
              from {S}.tarefas group by ferramenta order by ferramenta""").show()
print("quarentena (tarefas):", spark.table(f"{S}.quarentena").filter("tabela = 'tarefas'").groupBy("tratamento").count().collect())
