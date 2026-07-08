# je lis les trois journaux de commandes directement depuis hdfs, je calcule les indicateurs par entrepot et par jour
# puis j'ecris le resultat en parquet dans hdfs, aucun fichier ne passe par le disque local
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

spark = SparkSession.builder.appName("agregation_ventes").getOrCreate()

# je type chaque colonne moi meme plutot que de laisser spark deviner
schema = StructType([
    StructField("id_commande", StringType(), False),
    StructField("date", DateType(), False),
    StructField("client_id", StringType(), False),
    StructField("produit", StringType(), False),
    StructField("categorie", StringType(), False),
    StructField("quantite", IntegerType(), False),
    StructField("prix_unitaire", DoubleType(), False),
    StructField("entrepot", StringType(), False),
])

df = spark.read.option("header", True).schema(schema).csv("hdfs://namenode:9000/data/commandes")

# le montant d'une commande vaut la quantite multipliee par le prix unitaire
df = df.withColumn("montant", F.col("quantite") * F.col("prix_unitaire"))

agregats = (
    df.groupBy("entrepot", "date")
    .agg(
        F.round(F.sum("montant"), 2).alias("chiffre_affaires"),
        F.count("id_commande").alias("nb_commandes"),
        F.round(F.avg("montant"), 2).alias("panier_moyen"),
    )
    .orderBy("entrepot", "date")
)

agregats.show(20, truncate=False)

agregats.write.mode("overwrite").parquet("hdfs://namenode:9000/resultats/ventes_agregees")

spark.stop()
