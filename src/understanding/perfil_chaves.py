# Databricks notebook source
# Perfilamento que fundamenta as chaves de idempotência e as marcas d'água da bronze.
# Para cada tabela: chave candidata é única? Para cada coluna de data: está em ISO? qual o intervalo?
# Roda sobre a bronze (`workspace.bronze`); resultado documentado em docs/chaves-idempotencia.md.

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "bronze")
B = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"

# tabela -> colunas da chave candidata (nas tabelas do Postgres)
CHAVES = {
    "pg_pmo_projetos": ["projeto_id"],
    "pg_pmo_status_semanal": ["projeto_id", "data_status"],
    "pg_rh_colaboradores": ["colaborador_id"],
    "pg_financeiro_custos": ["lancamento_id"],
    "pg_timesheet_apontamentos": ["colaborador_id", "projeto_id", "semana_inicio"],
    "pg_compras_entregas_fornecedor": ["entrega_id"],
}

linhas = []
for tabela, cols in CHAVES.items():
    expr = "concat_ws('|', " + ", ".join(cols) + ")"
    n, k = spark.sql(f"select count(*), count(distinct {expr}) from {B}.{tabela}").first()
    linhas.append((tabela, "+".join(cols), n, k, n - k))
for colecao in ["mongo_clickup_listas", "mongo_msproject_projetos", "mongo_excel_cronogramas"]:
    n, k = spark.sql(f"select count(*), count(distinct _chave) from {B}.{colecao}").first()
    linhas.append((colecao, "_chave (negócio)", n, k, n - k))

print("Unicidade da chave candidata")
spark.createDataFrame(linhas, "tabela string, chave string, linhas long, chaves_distintas long, duplicadas long").show(truncate=False)

# COMMAND ----------
# Colunas de data candidatas à marca d'água: formato ISO (comparação textual correta) e intervalo.
DATAS = {
    "pg_pmo_status_semanal": "data_status",
    "pg_financeiro_custos": "data",
    "pg_timesheet_apontamentos": "semana_inicio",
    "pg_compras_entregas_fornecedor": "data_pedido",
}
linhas = []
for tabela, col in DATAS.items():
    r = spark.sql(
        f"select count(*) as n, sum(case when {col} is null or {col} = '' then 1 else 0 end) as nulos, "
        f"sum(case when {col} rlike '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' then 0 else 1 end) as fora_iso, "
        f"min({col}) as minimo, max({col}) as maximo from {B}.{tabela}"
    ).first()
    linhas.append((tabela, col, r["n"], r["nulos"], r["fora_iso"], r["minimo"], r["maximo"]))

print("Colunas de marca d'água")
spark.createDataFrame(
    linhas, "tabela string, coluna string, linhas long, nulos long, fora_do_iso long, minimo string, maximo string"
).show(truncate=False)

# COMMAND ----------
# Os duplicados de colaborador_id: quais ids e se as linhas são idênticas (mesmo hash) ou divergentes.
print("colaborador_id repetidos")
spark.sql(f"""
    select colaborador_id, count(*) as linhas, count(distinct _hash) as versoes_distintas, collect_set(nome) as nomes
    from {B}.pg_rh_colaboradores group by colaborador_id having count(*) > 1
""").show(truncate=False)
