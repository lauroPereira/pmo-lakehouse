# Databricks notebook source
# Simula mudanças nas origens para demonstrar a ingestão incremental da bronze:
#   Postgres: 2 custos novos (um na data mais recente, um retroativo dentro da janela) e 1 projeto alterado.
#   Mongo: 1 linha de cronograma Excel nova e 1 lista do ClickUp alterada.
# Esperado na bronze: 5 linhas inseridas. Para restaurar as origens, rode `carregar_origens`.

# COMMAND ----------
# MAGIC %pip install pymongo==4.10.1 psycopg2-binary==2.9.10 --quiet

# COMMAND ----------
from urllib.parse import quote_plus

import psycopg2
from pymongo import MongoClient


def secret(key):
    return dbutils.secrets.get("pmo", key)


conn = psycopg2.connect(
    host=secret("pg_host"), port=secret("pg_port"), dbname=secret("pg_db"),
    user=secret("pg_user"), password=secret("pg_password"), sslmode="require",
)
cur = conn.cursor()
cur.execute(
    "insert into financeiro.custos values "
    "('L9999991','PRJ-001','2026-10-16','pessoal','Lançamento simulado (novo)','1000.00','CC-001-PES'),"
    "('L9999992','PRJ-001','2026-10-01','pessoal','Lançamento simulado (retroativo)','2000.00','CC-001-PES')"
)
cur.execute("update pmo.projetos set gerente_projeto = gerente_projeto || ' (simulado)' where projeto_id = 'PRJ-001'")
conn.commit()
conn.close()

uri = f"mongodb+srv://{quote_plus(secret('mongo_user'))}:{quote_plus(secret('mongo_password'))}@{secret('mongo_host')}/"
db = MongoClient(uri, serverSelectionTimeoutMS=15000)["vertice"]
db.excel_cronogramas.insert_one({"_arquivo": "SIMULADO.xlsx", "_aba": "Cronograma", "_linha": 2, "ID": 1, "Tarefa": "Linha simulada"})
db.clickup_listas.update_one({}, {"$set": {"_simulado": True}})
print("Mudanças simuladas: 2 custos, 1 projeto, 1 linha Excel, 1 lista ClickUp")
