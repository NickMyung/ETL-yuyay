import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Prueba_1").getOrCreate()
print(spark)

"""
Archivos .Parquet
"""
df_brand2 = spark.read.parquet("parquet_data/Brand.parquet")
df_brand2.show()
print("Cantidad de Marcas registradas: " + str(df_brand2.count()))