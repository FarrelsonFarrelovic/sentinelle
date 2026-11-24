"""J1 — Chargement de l'historique dans le lakehouse Iceberg.

À lancer dans le conteneur spark :  spark-submit /opt/sentinelle/spark/j1_bronze_argent.py
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("sentinelle-j1").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.argent")

# ---------------------------------------------------------------- BRONZE
brut = (spark.read.parquet("/data/historique/")
        .withColumn("_ingere_le", F.current_timestamp())
        .withColumn("_fichier", F.col("_metadata.file_path")))
brut.writeTo("lake.bronze.transactions").createOrReplace()

comptes_brut = (spark.read.option("header", True).csv("/data/comptes.csv")
                .withColumn("_ingere_le", F.current_timestamp()))
comptes_brut.writeTo("lake.bronze.comptes").createOrReplace()

# ---------------------------------------------------------------- ARGENT
TYPES = ["PAIEMENT", "TRANSFERT", "RETRAIT", "DEPOT", "FACTURE", "RECHARGE"]

tx = (spark.table("lake.bronze.transactions")
      .select(
          F.col("tx_id").cast("string"),
          F.col("ts").cast("timestamp"),
          F.col("compte_id").cast("string"),
          F.col("type").cast("string"),
          F.col("montant").cast("decimal(18,2)"),
          F.col("devise").cast("string"),
          F.col("compte_dest").cast("string"),
          F.col("agent_id").cast("string"),
          F.col("pays").cast("string"),
          F.col("ville").cast("string"),
          F.col("canal").cast("string"),
          F.col("device_id").cast("string"),
          F.col("est_fraude").cast("int"),
          F.col("_ingere_le"))
      .filter((F.col("montant") > 0) & F.col("type").isin(TYPES) & F.col("tx_id").isNotNull()))

tx = tx.dropDuplicates(["tx_id"])

comptes = (spark.table("lake.bronze.comptes")
           .select("compte_id", "segment",
                   F.col("kyc_niveau").cast("int").alias("kyc_niveau"),
                   F.col("device_principal")))
argent = tx.join(F.broadcast(comptes), "compte_id", "left")

(argent.writeTo("lake.argent.transactions")
       .partitionedBy(F.days("ts"))
       .createOrReplace())

# ---------------------------------------------------------------- CONTRÔLES
n_bronze = spark.table("lake.bronze.transactions").count()
n_argent = spark.table("lake.argent.transactions").count()
n_dist = spark.table("lake.argent.transactions").select("tx_id").distinct().count()
print(f"{n_bronze} lignes en bronze, {n_argent} en argent, {n_dist} tx_id distincts")
assert n_argent == n_dist, "doublons en argent !"

# ---------------------------------------------------------------- PLAN (explain)
requete = (spark.table("lake.argent.transactions")
           .filter((F.col("ts") >= "2026-10-01") & (F.col("ts") < "2026-10-02"))
           .groupBy("segment").agg(F.sum("montant").alias("total"),
                                   F.count("*").alias("n")))
requete.explain("formatted")
requete.show()
