# Databricks notebook source
# MAGIC %md
# MAGIC # Gold · valor agregado e farol (CRISP-DM fase 4: modelagem)
# MAGIC
# MAGIC Aplica o modelo de gerenciamento do valor agregado (PMBOK, seção 5.1 de `01_entendimento_negocio`) sobre a silver.
# MAGIC O modelo é declarado como uma sequência de `Etapa`s em SQL, cada uma uma view que lê a anterior; os KPIs são definidos
# MAGIC uma vez só e publicados em duas tabelas.
# MAGIC
# MAGIC | Etapa | View / tabela | O que calcula |
# MAGIC |---|---|---|
# MAGIC | 1. Configuração | — | catálogo e **faixas** de situação e farol |
# MAGIC | 2. Bases | `horas`, `custo`, `farol` | horas totais, agregadas e planejadas por projeto; custo real; farol declarado na data de corte |
# MAGIC | 3. Valor agregado | `valor_agregado` | BAC, PV, EV, AC e % concluído e planejado (só projetos em andamento) |
# MAGIC | 4. Índices | `indices` | SPI, CPI, SV, CV |
# MAGIC | 5. Projeções e classificação | `gold.kpi_projeto` | EAC, ETC, VAC, estouro previsto, situação, farol calculado, melancia |
# MAGIC | 6. Carteira | `gold.kpi_carteira` | o resumo para a diretoria |
# MAGIC
# MAGIC Fórmulas: EV = BAC × horas agregadas ÷ horas totais; PV = BAC × horas planejadas ÷ horas totais; SPI = EV ÷ PV;
# MAGIC CPI = EV ÷ AC; EAC = BAC ÷ CPI; estouro = max(0, EAC − BAC).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

from dataclasses import dataclass

dbutils.widgets.text("catalog", "workspace")
CATALOG = dbutils.widgets.get("catalog")
S, G = f"{CATALOG}.silver", f"{CATALOG}.gold"
spark.sql(f"create schema if not exists {G}")

# Faixas (premissa a validar com o PMO): abaixo de CRITICO é crítico; abaixo de ATENCAO é atenção.
# O farol calculado usa as mesmas faixas sobre o SPI: verde >= ATENCAO, amarelo >= CRITICO, vermelho abaixo.
CRITICO, ATENCAO = 0.8, 0.9
FAROL_CALCULADO = f"case when spi >= {ATENCAO} then 'verde' when spi >= {CRITICO} then 'amarelo' else 'vermelho' end"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 a 5. O modelo, etapa por etapa

# COMMAND ----------


@dataclass(frozen=True)
class Etapa:
    nome: str        # nome da view temporária
    descricao: str
    sql: str


ETAPAS = [
    Etapa("horas", "2. Horas por projeto, somadas das tarefas da silver", f"""
        select projeto_id, max(data_corte) as data_corte,
               sum(horas_previstas) as horas_totais,
               sum(horas_agregadas) as horas_agregadas,
               sum(horas_planejadas) as horas_planejadas,
               count(*) as tarefas, sum(cast(atrasada as int)) as tarefas_atrasadas
        from {S}.tarefas group by projeto_id"""),
    Etapa("custo", "2. Custo real (AC) por projeto, estornos inclusos", f"""
        select projeto_id, sum(valor) as ac from {S}.custos group by projeto_id"""),
    Etapa("farol", "2. Farol declarado pelo gerente na data de corte", f"""
        select s.projeto_id, s.farol as farol_declarado, s.pct_concluido_declarado
        from {S}.status_semanal s join horas h on s.projeto_id = h.projeto_id and s.data_status = h.data_corte"""),
    Etapa("valor_agregado", "3. BAC, PV, EV e AC dos projetos em andamento", f"""
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
        where p.status = 'em_andamento'"""),
    Etapa("indices", "4. Índices de desempenho", """
        select *, ev / pv as spi, ev / ac as cpi, ev - pv as sv, ev - ac as cv from valor_agregado"""),
    Etapa("kpi_projeto", "5. Projeções, situação e farol calculado", f"""
        select *,
               bac / cpi as eac,
               bac / cpi - ac as etc,
               bac - bac / cpi as vac,
               greatest(0, bac / cpi - bac) as estouro_previsto,
               case when least(spi, cpi) < {CRITICO} then 'crítico'
                    when least(spi, cpi) < {ATENCAO} then 'atenção' else 'ok' end as situacao,
               {FAROL_CALCULADO} as farol_calculado,
               farol_declarado = 'verde' and ({FAROL_CALCULADO}) <> 'verde' as melancia
        from indices"""),
]


def executar(etapa: Etapa):
    spark.sql(f"create or replace temp view {etapa.nome} as {etapa.sql}")
    print(f"{etapa.descricao}: {spark.table(etapa.nome).count()} linhas")


for etapa in ETAPAS:
    executar(etapa)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publicação
# MAGIC
# MAGIC A última etapa vira `gold.kpi_projeto`; a etapa 6 agrega a carteira em `gold.kpi_carteira`.

# COMMAND ----------

spark.sql(f"""
    create or replace table {G}.kpi_projeto
    comment 'Valor agregado por projeto em andamento na data de corte (D1, D2, D3). Fonte: silver.'
    as select * from kpi_projeto
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Glossário no Unity Catalog
# MAGIC
# MAGIC Cada coluna publicada recebe a sua definição como comentário. O dashboard, o Genie e quem consulta a gold leem o
# MAGIC mesmo glossário, mantido num lugar só.

# COMMAND ----------

COMENTARIOS = {
    "kpi_projeto": {
        "projeto_id": "Identificador do projeto (PRJ-xxx)",
        "cliente": "Cliente do projeto",
        "segmento": "Segmento do cliente: varejo, financeiro, saude, logistica, industria",
        "ferramenta": "Ferramenta de cronograma do gerente: excel, clickup, msproject",
        "gerente": "Gerente do projeto",
        "data_corte": "Data de corte: último status semanal; os índices valem para esta data",
        "tarefas": "Quantidade de tarefas do cronograma",
        "tarefas_atrasadas": "Tarefas com % concluído abaixo do % planejado na data de corte",
        "bac": "BAC (Budget at Completion): orçamento de custo do projeto, em R$",
        "pv": "PV (Planned Value, valor planejado): parte do orçamento que deveria estar entregue na data de corte, em R$",
        "ev": "EV (Earned Value, valor agregado): parte do orçamento efetivamente entregue, em R$",
        "ac": "AC (Actual Cost, custo real): soma dos custos lançados no projeto, estornos inclusos, em R$",
        "pct_concluido_calculado": "% concluído calculado pelas horas das tarefas (0 a 100)",
        "pct_planejado": "% que deveria estar concluído na data de corte, pelo baseline (0 a 100)",
        "farol_declarado": "Farol informado pelo gerente no status semanal: verde, amarelo ou vermelho",
        "pct_concluido_declarado": "% concluído informado pelo gerente no status semanal",
        "spi": "SPI (Schedule Performance Index, índice de desempenho de prazo) = EV / PV; abaixo de 1 é atraso",
        "cpi": "CPI (Cost Performance Index, índice de desempenho de custo) = EV / AC; abaixo de 1 é custo acima do previsto",
        "sv": "SV (Schedule Variance, variação de prazo) = EV - PV, em R$",
        "cv": "CV (Cost Variance, variação de custo) = EV - AC, em R$",
        "eac": "EAC (Estimate at Completion, estimativa no término) = BAC / CPI: custo final previsto, em R$",
        "etc": "ETC (Estimate to Complete, estimativa para terminar) = EAC - AC: quanto ainda falta gastar, em R$",
        "vac": "VAC (Variance at Completion, variação no término) = BAC - EAC; negativo é estouro, em R$",
        "estouro_previsto": "Estouro previsto = max(0, EAC - BAC): quanto o projeto vai custar além do orçamento, em R$",
        "situacao": f"crítico se SPI ou CPI < {CRITICO}; atenção se < {ATENCAO}; senão ok",
        "farol_calculado": f"Farol pelo SPI: verde >= {ATENCAO}, amarelo >= {CRITICO}, vermelho abaixo",
        "melancia": "Verdadeiro quando o farol declarado é verde mas o farol calculado não é (verde por fora, atrasado por dentro)",
    },
    "kpi_carteira": {
        "data_corte": "Data de corte: último status semanal",
        "projetos_em_andamento": "Projetos em andamento na data de corte",
        "orcamento_total": "Soma do BAC (orçamento) dos projetos em andamento, em R$",
        "custo_real_total": "Soma do AC (custo real) dos projetos em andamento, em R$",
        "estouro_previsto_total": "Soma do estouro previsto (EAC - BAC) dos projetos que estouram, em R$",
        "projetos_criticos": f"Projetos com SPI ou CPI abaixo de {CRITICO}",
        "projetos_em_atencao": f"Projetos com SPI ou CPI abaixo de {ATENCAO}, sem ser críticos",
        "projetos_melancia": "Projetos com farol declarado verde e farol calculado diferente de verde",
        "maior_estouro_projeto": "Projeto com o maior estouro previsto",
        "maior_estouro_valor": "Valor do maior estouro previsto, em R$",
    },
}

for tabela, colunas in COMENTARIOS.items():
    for coluna, texto in colunas.items():
        texto = texto.replace("'", "\\'")
        spark.sql(f"alter table {G}.{tabela} alter column {coluna} comment '{texto}'")
    print(f"{tabela}: {len(colunas)} colunas comentadas")

# COMMAND ----------

spark.sql(f"""select projeto_id, round(spi, 2) spi, round(cpi, 2) cpi, round(estouro_previsto / 1e6, 2) estouro_mi,
                     situacao, farol_declarado, farol_calculado, melancia
              from {G}.kpi_projeto order by spi limit 8""").show(truncate=False)
spark.table(f"{G}.kpi_carteira").show(truncate=False)
