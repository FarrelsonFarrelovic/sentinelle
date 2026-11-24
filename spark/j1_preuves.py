"""J1 — Preuves pour le contrôle : comptages, partitions, plans. Lecture seule."""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("sentinelle-j1-preuves").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\n===== 1. COMPTAGES =====")
nb = spark.table("lake.bronze.transactions").count()
na = spark.table("lake.argent.transactions").count()
nd = spark.table("lake.argent.transactions").select("tx_id").distinct().count()
print(f"bronze={nb}  argent={na}  tx_id distincts en argent={nd}  doublons={na - nd}")

print("\n===== 2. PARTITIONS (une par jour) =====")
tot = spark.sql("SELECT count(DISTINCT _file) FROM lake.argent.transactions").first()[0]
jour = spark.sql("SELECT count(DISTINCT _file) FROM lake.argent.transactions "
                 "WHERE ts >= '2026-10-01' AND ts < '2026-10-02'").first()[0]
npart = spark.sql("SELECT count(DISTINCT _partition) FROM lake.argent.transactions").first()[0]
print(f"{npart} partitions ; {tot} fichiers au total ; {jour} fichier(s) lu(s) pour le 2026-10-01")

print("\n===== 3. PLAN : filtre sur un jour (élagage des partitions) =====")
q = (spark.table("lake.argent.transactions")
     .filter((F.col("ts") >= "2026-10-01") & (F.col("ts") < "2026-10-02"))
     .groupBy("segment").agg(F.sum("montant").alias("total"), F.count("*").alias("n")))
q.explain("formatted")

print("\n===== 4. PLAN : jointure transactions x comptes (broadcast) =====")
tx = spark.table("lake.bronze.transactions").filter(F.col("ts") < "2026-09-02")
cp = spark.table("lake.bronze.comptes")
tx.join(F.broadcast(cp), "compte_id", "left").explain("formatted")
