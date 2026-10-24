# Databricks notebook source
# MAGIC %md
# MAGIC # Gold · valor agregado e farol (CRISP-DM fase 4: modelagem)
# MAGIC
# MAGIC Aplica o modelo de gerenciamento do valor agregado (PMBOK, seção 5.1 de `01_entendimento_negocio`) sobre a silver.
# MAGIC Cada KPI é definido uma vez, aqui, e respondido nas tabelas:
# MAGIC
# MAGIC | Tabela | Grão | Responde |
# MAGIC |---|---|---|
# MAGIC | `gold.kpi_projeto` | 1 linha por projeto em andamento, na data de corte | D1 (SPI), D2 (CPI, EAC, estouro), D3 (farol declarado × calculado) |
# MAGIC | `gold.kpi_carteira` | 1 linha, a carteira inteira | o resumo para a diretoria |
# MAGIC
# MAGIC **Regras do modelo**
# MAGIC - BAC = orçamento de custo; EV = BAC × horas agregadas ÷ horas totais; PV = BAC × horas planejadas ÷ horas totais;
# MAGIC   AC = soma dos custos (estornos inclusos).
# MAGIC - SPI = EV ÷ PV; CPI = EV ÷ AC; EAC = BAC ÷ CPI (o desempenho de custo atual continua); estouro = max(0, EAC − BAC).
# MAGIC - Situação: crítico se min(SPI, CPI) < 0,8; atenção se < 0,9; senão ok.
# MAGIC - Farol calculado pelo SPI: verde ≥ 0,9; amarelo ≥ 0,8; vermelho < 0,8. **Melancia** = farol declarado verde e
# MAGIC   farol calculado diferente de verde.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
CATALOG = dbutils.widgets.get("catalog")
S, G = f"{CATALOG}.silver", f"{CATALOG}.gold"
spark.sql(f"create schema if not exists {G}")

spark.sql(f"""
create or replace table {G}.kpi_projeto
comment 'Valor agregado por projeto em andamento na data de corte (D1, D2, D3). Fonte: silver.'
as
with horas as (
    select projeto_id, max(data_corte) as data_corte,
           sum(horas_previstas) as horas_totais,
           sum(horas_agregadas) as horas_agregadas,
           sum(horas_planejadas) as horas_planejadas,
           count(*) as tarefas, sum(cast(atrasada as int)) as tarefas_atrasadas
    from {S}.tarefas group by projeto_id
),
custo as (
    select projeto_id, sum(valor) as ac from {S}.custos group by projeto_id
),
farol as (
    select s.projeto_id, s.farol as farol_declarado, s.pct_concluido_declarado
    from {S}.status_semanal s join horas h on s.projeto_id = h.projeto_id and s.data_status = h.data_corte
),
ev as (
    select p.projeto_id, p.cliente, p.segmento, p.ferramenta, p.gerente, h.data_corte,
           h.tarefas, h.tarefas_atrasadas,
           cast(p.orcamento_custo as double) as bac,
           cast(p.orcamento_custo as double) * h.horas_planejadas / h.horas_totais as pv,
           cast(p.orcamento_custo as double) * h.horas_agregadas / h.horas_totais as ev,
           cast(c.ac as double) as ac,
           100 * h.horas_agregadas / h.horas_totais as pct_concluido_calculado,
           100 * h.horas_planejadas / h.horas_totais as pct_planejado,
           f.farol_declarado, f.pct_concluido_declarado
    from {S}.projetos p
    join horas h using (projeto_id)
    join custo c using (projeto_id)
    left join farol f using (projeto_id)
    where p.status = 'em_andamento'
),
indices as (
    select *, ev / pv as spi, ev / ac as cpi, ev - pv as sv, ev - ac as cv from ev
)
select *,
       bac / cpi as eac,
       bac / cpi - ac as etc,
       bac - bac / cpi as vac,
       greatest(0, bac / cpi - bac) as estouro_previsto,
       case when least(spi, cpi) < 0.8 then 'crítico' when least(spi, cpi) < 0.9 then 'atenção' else 'ok' end as situacao,
       case when spi >= 0.9 then 'verde' when spi >= 0.8 then 'amarelo' else 'vermelho' end as farol_calculado,
       farol_declarado = 'verde'
         and (case when spi >= 0.9 then 'verde' when spi >= 0.8 then 'amarelo' else 'vermelho' end) <> 'verde'
         as melancia
from indices
""")

spark.sql(f"""
create or replace table {G}.kpi_carteira
comment 'Resumo da carteira em andamento na data de corte.'
as
select max(data_corte) as data_corte,
       count(*) as projetos_em_andamento,
       sum(bac) as orcamento_total,
       sum(ac) as custo_real_total,
       sum(estouro_previsto) as estouro_previsto_total,
       count_if(situacao = 'crítico') as projetos_criticos,
       count_if(situacao = 'atenção') as projetos_em_atencao,
       count_if(melancia) as projetos_melancia,
       max_by(projeto_id, estouro_previsto) as maior_estouro_projeto,
       max(estouro_previsto) as maior_estouro_valor
from {G}.kpi_projeto
""")

spark.sql(f"""select projeto_id, round(spi, 2) spi, round(cpi, 2) cpi, round(estouro_previsto / 1e6, 2) estouro_mi,
                     situacao, farol_declarado, farol_calculado, melancia
              from {G}.kpi_projeto order by spi limit 8""").show(truncate=False)
spark.table(f"{G}.kpi_carteira").show(truncate=False)
