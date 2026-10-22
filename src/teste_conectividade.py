# Databricks notebook source
# Teste de conectividade da Free Edition (serverless) com as origens externas.
# Segredos esperados no escopo `pmo`: pg_host, pg_port, pg_db, pg_user, pg_password, mongo_uri

# COMMAND ----------
# MAGIC %pip install pymongo[srv]==4.10.1 psycopg2-binary==2.9.10 --quiet

# COMMAND ----------
import socket
import time


def secret(key):
    return dbutils.secrets.get("pmo", key)


def tcp(host, port, timeout=8):
    t = time.time()
    try:
        socket.create_connection((host, int(port)), timeout=timeout).close()
        return f"OK ({time.time() - t:.1f}s)"
    except Exception as e:
        return f"FALHOU: {type(e).__name__}: {e}"


# COMMAND ----------
# Postgres (Supabase: usar o host do Session pooler, que é IPv4)
pg_host, pg_port = secret("pg_host"), secret("pg_port")
print("Postgres TCP:", tcp(pg_host, pg_port))

import psycopg2

try:
    conn = psycopg2.connect(
        host=pg_host, port=pg_port, dbname=secret("pg_db"),
        user=secret("pg_user"), password=secret("pg_password"),
        sslmode="require", connect_timeout=10,
    )
    cur = conn.cursor()
    cur.execute("select version()")
    print("Postgres SQL: OK ->", cur.fetchone()[0][:60])
    conn.close()
except Exception as e:
    print("Postgres SQL: FALHOU:", type(e).__name__, e)

# COMMAND ----------
# MongoDB Atlas
from pymongo import MongoClient

try:
    client = MongoClient(secret("mongo_uri"), serverSelectionTimeoutMS=10000)
    print("Mongo: OK ->", client.admin.command("ping"))
except Exception as e:
    print("Mongo: FALHOU:", type(e).__name__, str(e)[:300])

# COMMAND ----------
# Controle: um host público qualquer, para distinguir bloqueio geral de bloqueio específico
print("Controle (pypi.org:443):", tcp("pypi.org", 443))
print("Controle (example.com:5432):", tcp("example.com", 5432))
