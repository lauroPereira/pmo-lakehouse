# Databricks notebook source
# Bronze: copia as origens externas para o Unity Catalog, sem limpar nada, de forma incremental e idempotente.
#   - Postgres (JDBC): uma tabela Delta por tabela de origem, colunas como vieram (texto).
#   - MongoDB (pymongo): uma tabela por coleção, com o documento inteiro em JSON na coluna `documento`.
#
# Idempotência: `_hash` = SHA-256 do conteúdo da linha (ou do documento sem `_id` e `_carregado_em`).
# A carga é um MERGE só de inserção por `_hash`: linha igual não faz nada; linha nova ou alterada entra
# como nova versão (histórico). `_chave` guarda a chave natural, para a silver escolher a versão vigente.
# Incremental: nos 4 fatos, lê só `data >= marca d'água - janela` (a janela pega lançamentos retroativos).
# Limite conhecido: exclusões na origem não são detectadas.
# Ao final grava `_controle_carga`: linhas lidas, inseridas e conferência de que todo hash lido está na bronze.

# COMMAND ----------
# MAGIC %pip install pymongo==4.10.1 --quiet

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import hashlib
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote_plus

from bson import json_util
from delta.tables import DeltaTable
from pymongo import MongoClient
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "bronze")
dbutils.widgets.text("janela_dias", "28")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
JANELA = int(dbutils.widgets.get("janela_dias"))

LOTE_ID = uuid.uuid4().hex[:12]
CARGA = datetime.now(timezone.utc)
ISO = re.compile(r"^\d{4}-\d{2}-\d{2}")


def secret(key):
    return dbutils.secrets.get("pmo", key)


spark.sql(f"create schema if not exists {CATALOG}.{SCHEMA}")


def destino(nome):
    return f"{CATALOG}.{SCHEMA}.{nome}"


def rastreio(df, origem):
    return (
        df.withColumn("_origem", F.lit(origem))
        .withColumn("_carregado_em", F.lit(CARGA).cast("timestamp"))
        .withColumn("_lote_id", F.lit(LOTE_ID))
    )


controle = []


def gravar(df, nome, origem):
    """MERGE só de inserção por _hash; registra o controle de carga."""
    df = rastreio(df, origem)
    alvo = destino(nome)
    lidas = df.count()
    if spark.catalog.tableExists(alvo):
        antes = spark.table(alvo).count()
        (DeltaTable.forName(spark, alvo).alias("t")
         .merge(df.alias("s"), "t._hash = s._hash")
         .whenNotMatchedInsertAll().execute())
    else:
        antes = 0
        df.write.saveAsTable(alvo)
    tabela = spark.table(alvo)
    depois = tabela.count()
    ausentes = df.select("_hash").distinct().join(tabela.select("_hash").distinct(), "_hash", "left_anti").count()
    controle.append((origem, alvo, lidas, antes, depois, depois - antes, ausentes))


# COMMAND ----------
# Postgres via JDBC. Chave natural de cada tabela; os 4 fatos têm também a coluna de data da marca d'água.
CHAVES_PG = {
    "pmo.projetos": ["projeto_id"],
    "pmo.status_semanal": ["projeto_id", "data_status"],
    "rh.colaboradores": ["colaborador_id"],
    "financeiro.custos": ["lancamento_id"],
    "timesheet.apontamentos": ["colaborador_id", "projeto_id", "semana_inicio"],
    "compras.entregas_fornecedor": ["entrega_id"],
}
MARCA_DAGUA = {
    "pmo.status_semanal": "data_status",
    "financeiro.custos": "data",
    "timesheet.apontamentos": "semana_inicio",
    "compras.entregas_fornecedor": "data_pedido",
}

jdbc_opts = {
    "url": f"jdbc:postgresql://{secret('pg_host')}:{secret('pg_port')}/{secret('pg_db')}?sslmode=require",
    "user": secret("pg_user"),
    "password": secret("pg_password"),
    "driver": "org.postgresql.Driver",
}


def consulta_pg(tabela, nome):
    """SQL de leitura: completo, ou só a partir da marca d'água menos a janela (fatos)."""
    coluna = MARCA_DAGUA.get(tabela)
    alvo = destino(nome)
    if coluna and spark.catalog.tableExists(alvo):
        maximo = spark.sql(f"select max({coluna}) as m from {alvo}").first()["m"]
        if maximo and ISO.match(maximo):
            inicio = (date.fromisoformat(maximo[:10]) - timedelta(days=JANELA)).isoformat()
            print(f"{tabela}: incremental a partir de {inicio} (marca d'água {maximo})")
            return f"select * from {tabela} where {coluna} >= '{inicio}'"
    print(f"{tabela}: leitura completa")
    return f"select * from {tabela}"


for tabela, chave in CHAVES_PG.items():
    nome = "pg_" + tabela.replace(".", "_")
    df = spark.read.format("jdbc").options(**jdbc_opts).option("query", consulta_pg(tabela, nome)).load()
    colunas = df.columns
    texto = lambda c: F.coalesce(F.col(c).cast("string"), F.lit("∅"))
    df = (
        df.withColumn("_hash", F.sha2(F.concat_ws("\u001f", *[texto(c) for c in colunas]), 256))
        .withColumn("_chave", F.concat_ws("|", *[F.col(c) for c in chave]))
    )
    gravar(df, nome, f"postgres:{tabela}")

# COMMAND ----------
# MongoDB via pymongo. `_id` é regenerado quando a origem é recarregada, então a chave é de negócio.
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
    gravar(spark.createDataFrame(linhas, SCHEMA_MONGO), "mongo_" + colecao, f"mongo:vertice.{colecao}")

# COMMAND ----------
# Controle de carga
ctrl = spark.createDataFrame(
    controle,
    "origem string, tabela_bronze string, linhas_lidas long, linhas_antes long, linhas_depois long, "
    "linhas_inseridas long, hashes_ausentes long",
)
ctrl = (
    ctrl.withColumn("confere", F.col("hashes_ausentes") == 0)
    .withColumn("_carregado_em", F.lit(CARGA).cast("timestamp"))
    .withColumn("_lote_id", F.lit(LOTE_ID))
)
ctrl.write.mode("append").saveAsTable(destino("_controle_carga"))
ctrl.drop("_carregado_em", "tabela_bronze").show(truncate=False)

divergentes = ctrl.filter(~F.col("confere")).count()
assert divergentes == 0, f"{divergentes} tabela(s) com linhas lidas que não chegaram à bronze"
print("Lote", LOTE_ID, "OK:", len(controle), "tabelas,", sum(c[5] for c in controle), "linhas inseridas")
