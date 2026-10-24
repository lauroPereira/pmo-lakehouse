# Databricks notebook source
# Extração: lê as origens externas **por inteiro** e grava um snapshot completo, em Parquet imutável, na landing zone:
#   /Volumes/<catalog>/raw/landing/<fonte>/<tabela>/dt_raw=AAAA-MM-DD/lote_<id>/part-*.parquet
# Cada execução é uma fotografia da origem naquele momento (inclusive quando nada mudou), o que permite
# reconstruir a bronze, auditar e detectar exclusões comparando snapshots.
# Cada linha leva `_hash` (SHA-256 do conteúdo), `_chave` (chave natural), `_extraido_em` e `_lote_id`.
# Controle: `bronze._controle_extracao` (linhas lidas na origem × linhas gravadas na landing).

# COMMAND ----------
# MAGIC %pip install pymongo==4.10.1 --quiet

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import hashlib
import uuid
from datetime import datetime, timezone
from urllib.parse import quote_plus

from bson import json_util
from pymongo import MongoClient
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema_controle", "bronze")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA_CONTROLE = dbutils.widgets.get("schema_controle")

LANDING = f"/Volumes/{CATALOG}/raw/landing"
LOTE_ID = uuid.uuid4().hex[:12]
EXTRACAO = datetime.now(timezone.utc)
DT_RAW = EXTRACAO.strftime("%Y-%m-%d")
controle = []


def secret(key):
    return dbutils.secrets.get("pmo", key)


def gravar_snapshot(df, fonte, nome, origem):
    """Grava o snapshot completo na landing e confere a contagem lida de volta."""
    df = (
        df.withColumn("_extraido_em", F.lit(EXTRACAO).cast("timestamp"))
        .withColumn("_lote_id", F.lit(LOTE_ID))
    )
    lidas = df.count()
    destino = f"{LANDING}/{fonte}/{nome}/dt_raw={DT_RAW}/lote_{LOTE_ID}"
    df.coalesce(1).write.mode("error").parquet(destino)
    gravadas = spark.read.parquet(destino).count()
    controle.append((origem, destino, lidas, gravadas))
    print(f"{origem}: lidas={lidas} gravadas={gravadas}")


# COMMAND ----------
# Postgres via JDBC
CHAVES_PG = {
    "pmo.projetos": ["projeto_id"],
    "pmo.status_semanal": ["projeto_id", "data_status"],
    "rh.colaboradores": ["colaborador_id"],
    "financeiro.custos": ["lancamento_id"],
    "timesheet.apontamentos": ["colaborador_id", "projeto_id", "semana_inicio"],
    "compras.entregas_fornecedor": ["entrega_id"],
}
jdbc_opts = {
    "url": f"jdbc:postgresql://{secret('pg_host')}:{secret('pg_port')}/{secret('pg_db')}?sslmode=require",
    "user": secret("pg_user"),
    "password": secret("pg_password"),
    "driver": "org.postgresql.Driver",
}

for tabela, chave in CHAVES_PG.items():
    df = spark.read.format("jdbc").options(**jdbc_opts).option("dbtable", tabela).load()
    texto = lambda c: F.coalesce(F.col(c).cast("string"), F.lit("∅"))
    df = (
        df.withColumn("_hash", F.sha2(F.concat_ws("\u001f", *[texto(c) for c in df.columns]), 256))
        .withColumn("_chave", F.concat_ws("|", *[F.col(c) for c in chave]))
    )
    gravar_snapshot(df, "postgres", tabela.replace(".", "_"), f"postgres:{tabela}")

# COMMAND ----------
# MongoDB via pymongo. `_id` muda a cada recarga da origem, então a chave é de negócio e o hash ignora `_id`.
uri = f"mongodb+srv://{quote_plus(secret('mongo_user'))}:{quote_plus(secret('mongo_password'))}@{secret('mongo_host')}/"
db = MongoClient(uri, serverSelectionTimeoutMS=15000)["vertice"]

CHAVES_MONGO = {
    "clickup_listas": lambda d: d["list"]["id"],
    "msproject_projetos": lambda d: d["_arquivo"],
    "excel_cronogramas": lambda d: f"{d['_arquivo']}|{d['_linha']}",
}
SCHEMA_MONGO = StructType([StructField(c, StringType()) for c in ["_chave", "_hash", "documento"]])
VOLATIL = {"_id", "_carregado_em"}

for colecao, funcao_chave in CHAVES_MONGO.items():
    linhas = []
    for doc in db[colecao].find():
        estavel = {k: v for k, v in doc.items() if k not in VOLATIL}
        hash_ = hashlib.sha256(json_util.dumps(estavel, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        linhas.append((str(funcao_chave(doc)), hash_, json_util.dumps(doc, ensure_ascii=False)))
    gravar_snapshot(spark.createDataFrame(linhas, SCHEMA_MONGO), "mongo", colecao, f"mongo:vertice.{colecao}")

# COMMAND ----------
# Controle de extração
spark.sql(f"create schema if not exists {CATALOG}.{SCHEMA_CONTROLE}")
ctrl = (
    spark.createDataFrame(controle, "origem string, pasta_landing string, linhas_lidas long, linhas_gravadas long")
    .withColumn("confere", F.col("linhas_lidas") == F.col("linhas_gravadas"))
    .withColumn("_extraido_em", F.lit(EXTRACAO).cast("timestamp"))
    .withColumn("_lote_id", F.lit(LOTE_ID))
)
ctrl.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA_CONTROLE}._controle_extracao")
assert ctrl.filter(~F.col("confere")).count() == 0, "linhas lidas na origem diferentes das gravadas na landing"
print("Lote", LOTE_ID, "OK:", sum(c[2] for c in controle), "linhas no snapshot")
