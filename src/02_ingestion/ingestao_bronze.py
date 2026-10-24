# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão · landing → bronze
# MAGIC
# MAGIC Lê os snapshots em Parquet da landing com **Auto Loader** e grava as tabelas Delta da bronze, sem limpar nada.
# MAGIC
# MAGIC | Etapa | O que faz |
# MAGIC |---|---|
# MAGIC | 1. Configuração | catálogo, caminhos da landing e dos checkpoints, identificação da carga |
# MAGIC | 2. Fontes | declara as 9 fontes: tabela da bronze ↔ pasta da landing |
# MAGIC | 3. Leitura | Auto Loader lê **só os arquivos novos** (o checkpoint lembra os já lidos) |
# MAGIC | 4. Enriquecimento | acrescenta `_arquivo` (Parquet de origem) e `_carregado_em` |
# MAGIC | 5. Gravação | `MERGE` só de inserção por `_hash`: conteúdo repetido não duplica, linha alterada vira nova versão |
# MAGIC | 6. Conferência | todo hash da landing precisa estar na bronze |
# MAGIC | 7. Registro | grava `_controle_carga` e falha se algo não conferir |
# MAGIC
# MAGIC Idempotência em dois níveis: por arquivo (checkpoint) e por conteúdo (hash). A bronze pode ser apagada e reconstruída
# MAGIC só a partir da landing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fontes
# MAGIC
# MAGIC Cada fonte declara de onde lê (pasta da landing) e para onde grava (tabela da bronze). Para incluir uma fonte nova,
# MAGIC basta acrescentar uma linha aqui: as etapas seguintes valem para todas.

# COMMAND ----------


@dataclass(frozen=True)
class Fonte:
    tabela: str  # nome da tabela na bronze
    pasta: str   # pasta na landing

    @property
    def alvo(self) -> str:
        return f"{CATALOG}.{SCHEMA}.{self.tabela}"

    @property
    def caminho_landing(self) -> str:
        return f"{LANDING}/{self.pasta}"

    @property
    def checkpoint(self) -> str:
        return f"{CHECKPOINTS}/{self.tabela}/checkpoint"

    @property
    def schema_location(self) -> str:
        return f"{CHECKPOINTS}/{self.tabela}/schema"


FONTES = [
    Fonte("pg_pmo_projetos", "postgres/pmo_projetos"),
    Fonte("pg_pmo_status_semanal", "postgres/pmo_status_semanal"),
    Fonte("pg_rh_colaboradores", "postgres/rh_colaboradores"),
    Fonte("pg_financeiro_custos", "postgres/financeiro_custos"),
    Fonte("pg_timesheet_apontamentos", "postgres/timesheet_apontamentos"),
    Fonte("pg_compras_entregas_fornecedor", "postgres/compras_entregas_fornecedor"),
    Fonte("mongo_clickup_listas", "mongo/clickup_listas"),
    Fonte("mongo_msproject_projetos", "mongo/msproject_projetos"),
    Fonte("mongo_excel_cronogramas", "mongo/excel_cronogramas"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 a 5. Leitura, enriquecimento e gravação
# MAGIC
# MAGIC Uma função por etapa. O streaming roda com `trigger(availableNow=True)`: processa o que está pendente e termina,
# MAGIC como um batch incremental dentro do job.

# COMMAND ----------


def ler(fonte: Fonte) -> DataFrame:
    """3. Leitura: Auto Loader sobre a pasta da fonte; só arquivos ainda não registrados no checkpoint."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", fonte.schema_location)
        .load(fonte.caminho_landing)
    )


def enriquecer(df: DataFrame) -> DataFrame:
    """4. Enriquecimento: de qual arquivo veio cada linha e quando entrou na bronze."""
    return (
        df.withColumn("_arquivo", F.col("_metadata.file_path"))
        .withColumn("_carregado_em", F.lit(CARGA).cast("timestamp"))
    )


def gravar_por_hash(fonte: Fonte):
    """5. Gravação: devolve a função de cada micro-lote, que insere só o conteúdo (hash) que ainda não existe."""

    def mesclar(lote: DataFrame, _batch_id: int):
        lote = lote.dropDuplicates(["_hash"]).withColumn("_origem", F.lit(fonte.tabela))
        if spark.catalog.tableExists(fonte.alvo):
            (DeltaTable.forName(spark, fonte.alvo).alias("t")
             .merge(lote.alias("s"), "t._hash = s._hash")
             .whenNotMatchedInsertAll().execute())
        else:
            lote.write.saveAsTable(fonte.alvo)

    return mesclar


def ingerir(fonte: Fonte):
    """Encadeia as etapas 3, 4 e 5 para uma fonte e espera terminar."""
    (
        enriquecer(ler(fonte))
        .writeStream.foreachBatch(gravar_por_hash(fonte))
        .option("checkpointLocation", fonte.checkpoint)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Conferência
# MAGIC
# MAGIC Fotografa a tabela e o checkpoint antes e depois da ingestão. Os contadores vêm de `cloud_files_state`, porque no
# MAGIC serverless a função do `foreachBatch` roda no servidor e não devolve variáveis ao notebook.

# COMMAND ----------


def linhas(fonte: Fonte) -> int:
    return spark.table(fonte.alvo).count() if spark.catalog.tableExists(fonte.alvo) else 0


def arquivos_processados(fonte: Fonte) -> int:
    """Arquivos que o Auto Loader já registrou no checkpoint (0 se ainda não existe)."""
    try:
        return spark.sql(f"select count(*) from cloud_files_state('{fonte.checkpoint}')").first()[0]
    except Exception:
        return 0


def hashes_ausentes(fonte: Fonte) -> int:
    """Hashes presentes na landing e ausentes na bronze (precisa ser 0)."""
    landing = spark.read.parquet(fonte.caminho_landing).select("_hash").distinct()
    bronze = spark.table(fonte.alvo).select("_hash").distinct()
    return landing.join(bronze, "_hash", "left_anti").count()


def conferir(fonte: Fonte) -> tuple:
    """Executa a ingestão da fonte entre duas fotografias e devolve a linha de controle."""
    linhas_antes, arquivos_antes = linhas(fonte), arquivos_processados(fonte)
    ingerir(fonte)
    linhas_depois = linhas(fonte)
    return (fonte.tabela, arquivos_processados(fonte) - arquivos_antes, linhas_antes, linhas_depois,
            linhas_depois - linhas_antes, hashes_ausentes(fonte))


controle = [conferir(fonte) for fonte in FONTES]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Registro
# MAGIC
# MAGIC Uma linha por tabela em `bronze._controle_carga`. Se algum hash da landing não chegou à bronze, o notebook falha e o
# MAGIC job para antes da silver.

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
