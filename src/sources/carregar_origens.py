# Databricks notebook source
# Carga das origens externas a partir dos dados brutos do Volume `workspace.raw.material`.
#   - Postgres (Supabase): 6 tabelas, uma por CSV, em schemas por área. Tudo como texto,
#     para preservar os defeitos de origem (vírgula decimal, datas em formatos mistos, duplicados).
#   - MongoDB (Atlas): ClickUp (JSON), MS Project (XML) e cronogramas Excel, sem limpeza.
# Idempotente: recria as tabelas e esvazia as coleções antes de carregar.
# Segredos no escopo `pmo`: pg_host, pg_port, pg_db, pg_user, pg_password, mongo_user, mongo_password, mongo_host

# COMMAND ----------
# MAGIC %pip install pymongo==4.10.1 psycopg2-binary==2.9.10 openpyxl --quiet

# COMMAND ----------
import csv
import glob
import io
import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import quote_plus

import openpyxl
import psycopg2
from pymongo import MongoClient

BASE = "/Volumes/workspace/raw/material"
CARGA = datetime.now(timezone.utc)


def secret(key):
    return dbutils.secrets.get("pmo", key)


# schema.tabela -> arquivo
TABELAS = {
    "pmo.projetos": "pmo/projetos.csv",
    "pmo.status_semanal": "pmo/status_semanal.csv",
    "rh.colaboradores": "rh/colaboradores.csv",
    "financeiro.custos": "financeiro/custos.csv",
    "timesheet.apontamentos": "timesheet/apontamentos.csv",
    "compras.entregas_fornecedor": "compras/entregas_fornecedor.csv",
}

# COMMAND ----------
# Postgres
conn = psycopg2.connect(
    host=secret("pg_host"), port=secret("pg_port"), dbname=secret("pg_db"),
    user=secret("pg_user"), password=secret("pg_password"),
    sslmode="require", connect_timeout=10,
)
conn.autocommit = False
cur = conn.cursor()

resumo_pg = {}
for tabela, arquivo in TABELAS.items():
    schema = tabela.split(".")[0]
    with open(f"{BASE}/{arquivo}", encoding="utf-8", newline="") as f:
        conteudo = f.read()
    colunas = next(csv.reader(io.StringIO(conteudo)))
    cur.execute(f"create schema if not exists {schema}")
    cur.execute(f"drop table if exists {tabela}")
    cols_sql = ", ".join(f'"{c}" text' for c in colunas)
    cur.execute(f"create table {tabela} ({cols_sql})")
    cur.copy_expert(f"copy {tabela} from stdin with (format csv, header true)", io.StringIO(conteudo))
    cur.execute(f"select count(*) from {tabela}")
    resumo_pg[tabela] = cur.fetchone()[0]
conn.commit()
conn.close()
print("Postgres:", resumo_pg)

# COMMAND ----------
# MongoDB
uri = f"mongodb+srv://{quote_plus(secret('mongo_user'))}:{quote_plus(secret('mongo_password'))}@{secret('mongo_host')}/"
db = MongoClient(uri, serverSelectionTimeoutMS=15000)["vertice"]


def xml_para_dict(el):
    tag = el.tag.split("}")[-1]
    filhos = list(el)
    if not filhos:
        return tag, (el.text or "")
    d = {}
    for filho in filhos:
        k, v = xml_para_dict(filho)
        if k in d:
            if not isinstance(d[k], list):
                d[k] = [d[k]]
            d[k].append(v)
        else:
            d[k] = v
    return tag, d


def recarregar(colecao, docs):
    db[colecao].delete_many({})
    if docs:
        db[colecao].insert_many(docs)
    return db[colecao].count_documents({})


# ClickUp: um documento por export de lista
docs = []
for caminho in sorted(glob.glob(f"{BASE}/clickup/*.json")):
    with open(caminho, encoding="utf-8") as f:
        d = json.load(f)
    d["_arquivo"] = os.path.basename(caminho)
    d["_carregado_em"] = CARGA
    docs.append(d)
n_clickup = recarregar("clickup_listas", docs)

# MS Project: um documento por XML (estrutura original)
docs = []
for caminho in sorted(glob.glob(f"{BASE}/msproject/*.xml")):
    _, d = xml_para_dict(ET.parse(caminho).getroot())
    docs.append({"_arquivo": os.path.basename(caminho), "_carregado_em": CARGA, "projeto": d})
n_msp = recarregar("msproject_projetos", docs)

# Excel: um documento por linha de cronograma, valores como vieram (serial, texto, % ou fração)
docs = []
for caminho in sorted(glob.glob(f"{BASE}/excel/*.xlsx")):
    wb = openpyxl.load_workbook(caminho, data_only=True)
    for ws in wb:
        linhas = ws.iter_rows(values_only=True)
        cab = [str(c).replace(".", "_") if c is not None else f"coluna_{i}" for i, c in enumerate(next(linhas))]
        for n, valores in enumerate(linhas, start=2):
            if all(v is None for v in valores):
                continue
            doc = {"_arquivo": os.path.basename(caminho), "_aba": ws.title, "_linha": n, "_carregado_em": CARGA}
            doc.update(dict(zip(cab, valores)))
            docs.append(doc)
n_excel = recarregar("excel_cronogramas", docs)

print("Mongo:", {"clickup_listas": n_clickup, "msproject_projetos": n_msp, "excel_cronogramas": n_excel})
