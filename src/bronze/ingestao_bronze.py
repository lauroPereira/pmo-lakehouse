# Databricks notebook source
# Bronze: lê os Parquet da landing com Auto Loader (só arquivos novos, via checkpoint) e grava no Delta.
# Cada linha leva `_arquivo` (o arquivo Parquet que a originou), `dt_raw` (partição da landing),
# `_origem`, `_carregado_em`, além de `_hash`, `_chave`, `_extraido_em` e `_lote_id` vindos da extração.
# Idempotência em dois níveis: por arquivo (checkpoint do Auto Loader) e por conteúdo (MERGE só de
# inserção por `_hash`). A bronze pode ser apagada e reconstruída só a partir da landing.
# Controle: `_controle_carga` (arquivos, linhas lidas e inseridas; todo hash da landing precisa estar na bronze).

# COMMAND ----------
import uuid
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "bronze")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

LANDING = f"/Volumes/{CATALOG}/raw/landing"
CHECKPOINTS = f"/Volumes/{CATALOG}/raw/checkpoints/bronze"
LOTE_CARGA = uuid.uuid4().hex[:12]
CARGA = datetime.now(timezone.utc)

spark.sql(f"create schema if not exists {CATALOG}.{SCHEMA}")

# tabela bronze -> pasta da landing
FONTES = {
    "pg_pmo_projetos": "postgres/pmo_projetos",
    "pg_pmo_status_semanal": "postgres/pmo_status_semanal",
    "pg_rh_colaboradores": "postgres/rh_colaboradores",
    "pg_financeiro_custos": "postgres/financeiro_custos",
    "pg_timesheet_apontamentos": "postgres/timesheet_apontamentos",
    "pg_compras_entregas_fornecedor": "postgres/compras_entregas_fornecedor",
    "mongo_clickup_listas": "mongo/clickup_listas",
    "mongo_msproject_projetos": "mongo/msproject_projetos",
    "mongo_excel_cronogramas": "mongo/excel_cronogramas",
}

# COMMAND ----------
controle = []


def arquivos_processados(ckpt):
    """Arquivos que o Auto Loader já registrou no checkpoint (0 se ainda não existe)."""
    try:
        return spark.sql(f"select count(*) from cloud_files_state('{ckpt}')").first()[0]
    except Exception:
        return 0


for nome, pasta in FONTES.items():
    alvo = f"{CATALOG}.{SCHEMA}.{nome}"
    ckpt = f"{CHECKPOINTS}/{nome}/checkpoint"

    def mesclar(micro, batch_id, alvo=alvo, nome=nome):
        micro = micro.dropDuplicates(["_hash"]).withColumn("_origem", F.lit(nome))
        if spark.catalog.tableExists(alvo):
            (DeltaTable.forName(spark, alvo).alias("t")
             .merge(micro.alias("s"), "t._hash = s._hash")
             .whenNotMatchedInsertAll().execute())
        else:
            micro.write.saveAsTable(alvo)

    antes = spark.table(alvo).count() if spark.catalog.tableExists(alvo) else 0
    arquivos_antes = arquivos_processados(ckpt)
    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/{nome}/schema")
        .load(f"{LANDING}/{pasta}")
        .withColumn("_arquivo", F.col("_metadata.file_path"))
        .withColumn("_carregado_em", F.lit(CARGA).cast("timestamp"))
        .writeStream.foreachBatch(mesclar)
        .option("checkpointLocation", ckpt)
        .trigger(availableNow=True)
        .start().awaitTermination()
    )
    tabela = spark.table(alvo)
    depois = tabela.count()
    ausentes = (
        spark.read.parquet(f"{LANDING}/{pasta}").select("_hash").distinct()
        .join(tabela.select("_hash").distinct(), "_hash", "left_anti").count()
    )
    controle.append((nome, arquivos_processados(ckpt) - arquivos_antes, antes, depois, depois - antes, ausentes))

# COMMAND ----------
ctrl = (
    spark.createDataFrame(
        controle,
        "tabela_bronze string, arquivos_novos long, linhas_antes long, linhas_depois long, "
        "linhas_inseridas long, hashes_ausentes long",
    )
    .withColumn("confere", F.col("hashes_ausentes") == 0)
    .withColumn("_carregado_em", F.lit(CARGA).cast("timestamp"))
    .withColumn("_lote_id", F.lit(LOTE_CARGA))
)
ctrl.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}._controle_carga")
ctrl.drop("_carregado_em").show(truncate=False)
assert ctrl.filter(~F.col("confere")).count() == 0, "há hashes da landing que não chegaram à bronze"
print("Lote", LOTE_CARGA, "OK:", sum(c[4] for c in controle), "linhas inseridas")
