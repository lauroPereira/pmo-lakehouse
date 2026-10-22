# Databricks notebook source
# MAGIC %md
# MAGIC # CRISP-DM · Fase 2 · Entendimento dos dados
# MAGIC
# MAGIC > Cenário simulado (Vértice Tecnologia, dados sintéticos da imersão Jornada de Dados). Ver a fase 1 em
# MAGIC > `01_entendimento_negocio`.
# MAGIC
# MAGIC **Objetivo:** conhecer as fontes que respondem às dores do escopo (D1–D3), medir os defeitos que ameaçam os KPIs e
# MAGIC **confirmar nos nossos dados** as evidências marcadas com † na fase 1. Tudo aqui lê a bronze (o dado como chegou);
# MAGIC os cálculos de KPI são um protótipo exploratório, que a silver e a gold vão formalizar.
# MAGIC
# MAGIC **Método:** inventário → perfil de cada fonte → defeitos encontrados → protótipo dos KPIs → esperado × encontrado →
# MAGIC regras de qualidade propostas para a silver.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
B = f"{dbutils.widgets.get('catalog')}.bronze"


def vigente(tabela):
    """Versão vigente de cada registro: a bronze guarda versões; vale a extração mais recente por chave natural."""
    return spark.sql(f"""
        select * from {B}.{tabela}
        qualify row_number() over (partition by _chave order by _extraido_em desc, _hash) = 1
    """)


for tabela in ["pg_pmo_projetos", "pg_pmo_status_semanal", "pg_financeiro_custos",
               "mongo_excel_cronogramas", "mongo_clickup_listas", "mongo_msproject_projetos"]:
    vigente(tabela).createOrReplaceTempView(tabela)

# Tarefas de cada ferramenta, abertas a partir do JSON cru da bronze, sem nenhuma correção
spark.sql("""
    create or replace temp view excel_tarefas as
    select regexp_extract(_chave, 'PRJ-[0-9]{3}', 0) as projeto_id, _chave,
           m['Tarefa'] as tarefa, m['Início previsto'] as inicio_previsto, m['Término previsto'] as termino_previsto,
           m['% Concluído'] as pct_concluido, m['Horas previstas'] as horas_previstas
    from (select _chave, from_json(documento, 'map<string,string>') as m from mongo_excel_cronogramas)
""")
spark.sql("""
    create or replace temp view clickup_tarefas as
    select get_json_object(documento, '$.list.id') as list_id, get_json_object(documento, '$.list.name') as list_name, t.*
    from mongo_clickup_listas
    lateral view explode(from_json(get_json_object(documento, '$.tasks'),
        'array<struct<id:string,name:string,status:struct<status:string,type:string>,time_estimate:bigint,
                      custom_fields:array<struct<name:string,type:string,value:string>>>>')) x as t
""")
spark.sql("""
    create or replace temp view msproject_tarefas as
    select regexp_extract(_chave, 'PRJ-[0-9]{3}', 0) as projeto_id, t.*
    from mongo_msproject_projetos
    lateral view explode(from_json(get_json_object(documento, '$.projeto.Tasks.Task'),
        'array<struct<UID:string,Name:string,OutlineLevel:string,Summary:string,PercentComplete:string,Work:string,
                      Baseline:struct<Start:string,Finish:string,Work:string>>>')) x as t
""")
DATA_CORTE = spark.sql("select max(data_status) from pg_pmo_status_semanal").first()[0]
print("Data de corte (último status semanal):", DATA_CORTE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inventário das fontes do escopo
# MAGIC
# MAGIC | Fonte (sistema) | Tabela na bronze | Granularidade | Chave natural | Serve a |
# MAGIC |---|---|---|---|---|
# MAGIC | Sistema corporativo · PMO | `pg_pmo_projetos` | 1 linha por projeto | `projeto_id` | D1, D2, D3 |
# MAGIC | Sistema corporativo · PMO | `pg_pmo_status_semanal` | 1 linha por projeto por sexta-feira | `projeto_id` + `data_status` | D3 |
# MAGIC | Sistema corporativo · financeiro | `pg_financeiro_custos` | 1 lançamento de custo | `lancamento_id` | D2 |
# MAGIC | Exports · Excel | `mongo_excel_cronogramas` | 1 linha de planilha | arquivo + linha | D1, D2 |
# MAGIC | Exports · ClickUp | `mongo_clickup_listas` | 1 lista (projeto) com as tarefas dentro | `list.id` | D1, D2 |
# MAGIC | Exports · MS Project | `mongo_msproject_projetos` | 1 cronograma (projeto) com as tarefas dentro | nome do arquivo | D1, D2 |
# MAGIC
# MAGIC **Data de corte:** as análises usam a data do último status semanal (sexta-feira, 16/10/2026) como "hoje".

# COMMAND ----------

display(spark.sql("""
    select 'projetos' as fonte, count(*) as registros from pg_pmo_projetos union all
    select 'status semanal', count(*) from pg_pmo_status_semanal union all
    select 'custos', count(*) from pg_financeiro_custos union all
    select 'Excel · linhas', count(*) from excel_tarefas union all
    select 'ClickUp · tarefas', count(*) from clickup_tarefas union all
    select 'MS Project · tarefas', count(*) from msproject_tarefas
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Chaves naturais (todas as fontes)
# MAGIC
# MAGIC A chave natural identifica um registro na origem; ela fundamenta a idempotência da bronze e a escolha da versão
# MAGIC vigente na silver (método em `docs/chaves-idempotencia.md`). O teste é feito no **snapshot mais recente da
# MAGIC landing**, isto é, na origem como foi entregue: `count(*)` contra `count(distinct chave)`.
# MAGIC
# MAGIC | Fonte | Chave candidata |
# MAGIC |---|---|
# MAGIC | `pmo.projetos` | `projeto_id` |
# MAGIC | `pmo.status_semanal` | `projeto_id` + `data_status` |
# MAGIC | `rh.colaboradores` | `colaborador_id` |
# MAGIC | `financeiro.custos` | `lancamento_id` |
# MAGIC | `timesheet.apontamentos` | `colaborador_id` + `projeto_id` + `semana_inicio` |
# MAGIC | `compras.entregas_fornecedor` | `entrega_id` |
# MAGIC | ClickUp · MS Project · Excel | `list.id` · nome do arquivo · arquivo + linha (o `_id` do Mongo muda a cada recarga e não serve) |

# COMMAND ----------

from pyspark.sql import functions as F

LANDING = f"/Volumes/{dbutils.widgets.get('catalog')}/raw/landing"
FONTES_LANDING = {
    "pmo.projetos": "postgres/pmo_projetos",
    "pmo.status_semanal": "postgres/pmo_status_semanal",
    "rh.colaboradores": "postgres/rh_colaboradores",
    "financeiro.custos": "postgres/financeiro_custos",
    "timesheet.apontamentos": "postgres/timesheet_apontamentos",
    "compras.entregas_fornecedor": "postgres/compras_entregas_fornecedor",
    "mongo.clickup_listas": "mongo/clickup_listas",
    "mongo.msproject_projetos": "mongo/msproject_projetos",
    "mongo.excel_cronogramas": "mongo/excel_cronogramas",
}


def ultimo_snapshot(pasta):
    df = spark.read.parquet(f"{LANDING}/{pasta}")
    return df.filter(F.col("_extraido_em") == df.agg(F.max("_extraido_em")).first()[0])


linhas = []
for fonte, pasta in FONTES_LANDING.items():
    n, k = ultimo_snapshot(pasta).agg(F.count("*"), F.countDistinct("_chave")).first()
    linhas.append((fonte, n, k, n - k))
display(spark.createDataFrame(linhas, "fonte string, linhas long, chaves_distintas long, duplicadas long"))

# O único caso com duplicadas: são registros diferentes (hash distinto) ou cópias idênticas?
display(
    ultimo_snapshot("postgres/rh_colaboradores")
    .groupBy("colaborador_id")
    .agg(F.count("*").alias("linhas"), F.countDistinct("_hash").alias("versoes_distintas"),
         F.collect_set("nome").alias("nomes"))
    .filter("linhas > 1")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado:** todas as chaves são únicas, exceto `colaborador_id`: 3 ids (COL0102, COL0257, COL0433) com 2 linhas
# MAGIC cada, que diferem só na caixa do nome (`Renata Monteiro` e `RENATA MONTEIRO`). A bronze guarda as duas versões; a
# MAGIC silver precisa normalizar o nome antes de deduplicar. Fica fora do escopo deste ciclo, mas registrado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Perfil das fontes e defeitos encontrados
# MAGIC
# MAGIC Cada consulta abaixo mede um defeito. Dado sujo não dá erro: dá número errado em silêncio.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Cadastro de projetos

# COMMAND ----------

display(spark.sql("""
    select ferramenta, count(*) as projetos, count(codigo_ferramenta) as com_codigo_da_ferramenta
    from pg_pmo_projetos group by ferramenta order by ferramenta
"""))
display(spark.sql("""
    select status, count(*) as projetos,
           round(sum(cast(valor_contrato as decimal(18,2))) / 1e6, 1) as carteira_milhoes,
           sum(case when data_inicio like '%/%' then 1 else 0 end) as data_inicio_com_barra
    from pg_pmo_projetos group by status
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Status semanal (o farol do gerente)

# COMMAND ----------

display(spark.sql("""
    select farol, lower(farol) as farol_normalizado, count(*) as registros
    from pg_pmo_status_semanal group by farol order by farol_normalizado, registros desc
"""))
display(spark.sql("""
    select count(*) as registros,
           sum(case when texto_status is null or trim(texto_status) = '' then 1 else 0 end) as texto_vazio,
           count(distinct projeto_id) filter (where data_status = (select max(data_status) from pg_pmo_status_semanal))
               as projetos_com_status_na_data_de_corte
    from pg_pmo_status_semanal
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Custos

# COMMAND ----------

display(spark.sql("""
    select count(*) as lancamentos,
           sum(case when valor like '%,%' then 1 else 0 end) as com_virgula_decimal,
           sum(case when cast(replace(valor, ',', '.') as decimal(18,2)) < 0 then 1 else 0 end) as negativos_estornos,
           sum(case when try_cast(valor as decimal(18,2)) is null then 1 else 0 end) as nao_convertem_sem_tratamento,
           round(sum(cast(replace(valor, ',', '.') as decimal(18,2))) / 1e6, 2) as custo_total_milhoes_tratado,
           round(sum(try_cast(valor as decimal(18,2))) / 1e6, 2) as custo_total_milhoes_ingenuo
    from pg_financeiro_custos
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 As três ferramentas de cronograma
# MAGIC
# MAGIC Cada ferramenta guarda as mesmas quatro informações de um jeito diferente:
# MAGIC
# MAGIC | Informação | Excel | ClickUp | MS Project |
# MAGIC |---|---|---|---|
# MAGIC | Projeto | no nome do arquivo (`PRJ-xxx`) | só o código da lista, ligado ao cadastro por `codigo_ferramenta` (ou pelo nome) | no nome do arquivo |
# MAGIC | Início e fim de baseline | `Início/Término previsto`: texto `dd/MM/aaaa` **ou** número de série do Excel | campos personalizados `Início/Fim baseline`, em milissegundos | `Baseline.Start/Finish` |
# MAGIC | % concluído | `"45%"`, `"64,4%"` **ou** fração `0.45` | campo personalizado `Progresso (%)` | `PercentComplete` |
# MAGIC | Horas previstas | número **ou** texto `"375h"` | `time_estimate` em milissegundos | `Work` em `PT40H0M0S` |
# MAGIC | Armadilha de soma | linha `TOTAL` no fim de cada planilha | — | tarefas-resumo (`Summary = 1`) somam as de baixo |

# COMMAND ----------

display(spark.sql("""
    select count(*) as linhas,
           sum(case when tarefa = 'TOTAL' then 1 else 0 end) as linhas_total,
           sum(case when inicio_previsto like '%/%' then 1 else 0 end) as inicio_em_texto,
           sum(case when inicio_previsto not like '%/%' then 1 else 0 end) as inicio_em_numero_de_serie,
           sum(case when pct_concluido like '%\\%%' then 1 else 0 end) as pct_com_simbolo,
           sum(case when pct_concluido not like '%\\%%' then 1 else 0 end) as pct_em_fracao,
           sum(case when horas_previstas like '%h' then 1 else 0 end) as horas_com_h
    from excel_tarefas
"""))
display(spark.sql("""
    select count(*) as tarefas, count(distinct status.status) as grafias_de_status,
           count(distinct list_id) as listas
    from clickup_tarefas
"""))
display(spark.sql("""
    select count(*) as tarefas, sum(case when Summary = '1' then 1 else 0 end) as tarefas_resumo
    from msproject_tarefas
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Protótipo dos KPIs e confirmação das evidências da fase 1
# MAGIC
# MAGIC As tarefas das três ferramentas empilhadas numa visão só, com as conversões mínimas para calcular SPI, CPI e EAC
# MAGIC (seção 5.1 da fase 1). É exploratório: a regra definitiva vai para a silver.

# COMMAND ----------

spark.sql(r"""
    create or replace temp view tarefas_unificadas as
    select projeto_id, 'excel' as ferramenta, tarefa,
           case when inicio_previsto like '%/%' then to_date(inicio_previsto, 'dd/MM/yyyy')
                else date_add(date'1899-12-30', cast(cast(inicio_previsto as double) as int)) end as inicio_baseline,
           case when termino_previsto like '%/%' then to_date(termino_previsto, 'dd/MM/yyyy')
                else date_add(date'1899-12-30', cast(cast(termino_previsto as double) as int)) end as fim_baseline,
           least(100, case when pct_concluido like '%\%%'
                           then cast(replace(replace(pct_concluido, '%', ''), ',', '.') as double)
                           else cast(pct_concluido as double) * 100 end) as pct_concluido,
           cast(replace(horas_previstas, 'h', '') as double) as horas
    from excel_tarefas where tarefa <> 'TOTAL'
    union all
    select p.projeto_id, 'clickup', c.name,
           to_date(timestamp_millis(cast(filter(c.custom_fields, f -> f.name = 'Início baseline')[0].value as bigint))),
           to_date(timestamp_millis(cast(filter(c.custom_fields, f -> f.name = 'Fim baseline')[0].value as bigint))),
           cast(filter(c.custom_fields, f -> f.name = 'Progresso (%)')[0].value as double),
           c.time_estimate / 3600000
    from clickup_tarefas c
    join pg_pmo_projetos p on p.codigo_ferramenta = c.list_id or p.nome_projeto = c.list_name
    union all
    select projeto_id, 'msproject', Name, to_date(Baseline.Start), to_date(Baseline.Finish),
           cast(PercentComplete as double), cast(regexp_extract(Work, 'PT([0-9]+)H', 1) as double)
    from msproject_tarefas where Summary = '0'
""")

spark.sql(f"""
    create or replace temp view kpi_prototipo as
    with valor as (
        select projeto_id,
               sum(horas) as horas_totais,
               sum(horas * pct_concluido / 100) as horas_agregadas,
               sum(horas * greatest(0, least(1, datediff(date'{DATA_CORTE}', inicio_baseline)
                                                / datediff(fim_baseline, inicio_baseline)))) as horas_planejadas
        from tarefas_unificadas group by projeto_id
    ),
    custo as (
        select projeto_id, sum(cast(replace(valor, ',', '.') as decimal(18,2))) as custo_real
        from pg_financeiro_custos group by projeto_id
    ),
    farol as (
        select projeto_id, lower(farol) as farol from pg_pmo_status_semanal where data_status = '{DATA_CORTE}'
    )
    select p.projeto_id, p.cliente,
           v.horas_agregadas / v.horas_planejadas as spi,
           cast(p.orcamento_custo as double) * v.horas_agregadas / v.horas_totais / c.custo_real as cpi,
           c.custo_real * v.horas_totais / v.horas_agregadas as eac,
           c.custo_real * v.horas_totais / v.horas_agregadas - cast(p.orcamento_custo as double) as estouro,
           f.farol
    from pg_pmo_projetos p
    join valor v using (projeto_id) join custo c using (projeto_id) join farol f using (projeto_id)
    where p.status = 'em_andamento'
""")
display(spark.sql("""
    select projeto_id, cliente, round(spi, 2) as spi, round(cpi, 2) as cpi,
           round(estouro / 1e6, 2) as estouro_milhoes, farol as farol_do_gerente
    from kpi_prototipo order by spi
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Esperado × encontrado
# MAGIC
# MAGIC Os valores "esperado" são os números de referência da imersão; "encontrado" é o que os nossos dados, vindos das
# MAGIC origens Postgres e MongoDB pela landing e pela bronze, produzem.

# COMMAND ----------

conferencia = spark.sql("""
    select item, esperado, encontrado, case when esperado = encontrado then 'confirmado' else 'DIVERGE' end as situacao
    from (
      select 1 as ordem, 'projetos em Excel · ClickUp · MS Project' as item, '20 · 25 · 15' as esperado,
             concat((select count(distinct projeto_id) from excel_tarefas), ' · ',
                    (select count(distinct list_id) from clickup_tarefas), ' · ',
                    (select count(distinct projeto_id) from msproject_tarefas)) as encontrado
      union all select 2, 'projetos em andamento', '41', cast(count(*) as string) from kpi_prototipo
      union all select 3, 'custos com vírgula decimal', '671',
             cast((select count(*) from pg_financeiro_custos where valor like '%,%') as string)
      union all select 4, 'custo real total (R$ mi)', '193.35',
             cast((select round(sum(cast(replace(valor, ',', '.') as decimal(18,2))) / 1e6, 2) from pg_financeiro_custos) as string)
      union all select 5, 'linhas TOTAL no Excel', '20',
             cast((select count(*) from excel_tarefas where tarefa = 'TOTAL') as string)
      union all select 6, 'tarefas-resumo no MS Project', '105',
             cast((select count(*) from msproject_tarefas where Summary = '1') as string)
      union all select 7, 'projetos críticos (SPI ou CPI < 0,8)', '7',
             cast((select count(*) from kpi_prototipo where least(spi, cpi) < 0.8) as string)
      union all select 8, 'projetos em atenção (< 0,9, sem crítico)', '10',
             cast((select count(*) from kpi_prototipo where least(spi, cpi) >= 0.8 and least(spi, cpi) < 0.9) as string)
      union all select 9, 'estouro previsto da carteira (R$ mi)', '6.47',
             cast((select round(sum(greatest(0, estouro)) / 1e6, 2) from kpi_prototipo) as string)
      union all select 10, 'maior estouro', 'PRJ-021',
             (select max_by(projeto_id, estouro) from kpi_prototipo)
      union all select 11, 'melancias (farol verde e SPI < 0,9)', 'PRJ-012, PRJ-028, PRJ-034, PRJ-042',
             (select array_join(array_sort(collect_list(projeto_id)), ', ') from kpi_prototipo where farol = 'verde' and spi < 0.9)
    ) order by ordem
""")
conferencia.drop("ordem").show(truncate=False)
display(conferencia)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Achados e regras de qualidade propostas para a silver
# MAGIC
# MAGIC | # | Achado | Ocorrências | Efeito se ignorado | Regra proposta na silver |
# MAGIC |---|---|---|---|---|
# MAGIC | Q1 | Ferramenta do projeto escrita de 8 jeitos | 8 grafias em 60 projetos | filtro por ferramenta perde projetos sem aviso | normalizar para `excel`, `clickup`, `msproject` |
# MAGIC | Q2 | Projeto sem código da ferramenta | 2 projetos | cronograma não encontrado | casar pelo nome; sem casamento → quarentena |
# MAGIC | Q3 | `data_inicio` em dois formatos | 24 projetos com `dd/MM/aaaa` | conta de prazo erra 24 projetos | converter os dois formatos; data inválida → quarentena |
# MAGIC | Q4 | Farol escrito em várias caixas | 9 grafias para 3 cores | farol "VERDE" não conta como verde | normalizar para minúsculas e validar o domínio |
# MAGIC | Q5 | Custo com vírgula decimal | 671 lançamentos | custo some da soma: R$ 187,94 mi em vez de R$ 193,35 mi (−R$ 5,41 mi) | trocar vírgula por ponto; valor não numérico → quarentena |
# MAGIC | Q6 | Custos negativos (estornos) | 331 lançamentos | — (legítimos) | manter, sinalizados como estorno |
# MAGIC | Q7 | Linha `TOTAL` nas planilhas | 20 linhas | horas contadas duas vezes | descartar, com registro do descarte |
# MAGIC | Q8 | Datas do Excel em texto ou número de série | início: 988 em texto, 655 em número de série | datas nulas ou erradas | converter os dois formatos |
# MAGIC | Q9 | % concluído do Excel em 3 formatos, às vezes > 100 | 1.126 com `%`, 517 em fração; 11 acima de 100 | avanço distorcido | converter para 0–100, limitar em 100 e registrar o corte |
# MAGIC | Q10 | Horas com sufixo `h`; ClickUp em ms; Project em `PT..H` | 43 linhas do Excel com `h`; todas as do ClickUp e do Project | horas nulas ou 1000× maiores | converter tudo para horas |
# MAGIC | Q11 | Tarefas-resumo do MS Project | 105 tarefas | horas contadas duas vezes | manter só `Summary = 0` |
# MAGIC | Q12 | Status das tarefas do ClickUp escrito de vários jeitos | 12 grafias | contagem por status distorcida | normalizar (não afeta SPI, que usa o % concluído) |
# MAGIC | Q13 | Status semanal com texto vazio | 14 registros | sem contexto do farol | manter, sinalizado (o texto entra em ciclos futuros) |
# MAGIC
# MAGIC Fora do escopo deste ciclo, mas registrado: 3 `colaborador_id` duplicados que diferem só na caixa do nome.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Conclusão da fase 2
# MAGIC
# MAGIC - As evidências da fase 1 se confirmam nos nossos dados (tabela esperado × encontrado): as dores D1–D3 são reais e
# MAGIC   mensuráveis.
# MAGIC - Os defeitos Q1–Q11 mostram por que o requisito não funcional (dado confiável) é condição para os KPIs: sem tratamento,
# MAGIC   custo, prazo e avanço mudam em silêncio.
# MAGIC - **Decisões para a fase 3 (silver):** um modelo único de tarefas para as 3 ferramentas, custos tipados, farol
# MAGIC   normalizado, e quarentena com motivo para o que não puder ser corrigido. A tabela esperado × encontrado vira teste
# MAGIC   automático a cada carga.
