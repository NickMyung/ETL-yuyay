import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Prueba_1").getOrCreate()
print(spark)

"""
Lectura de archivos .CSV
"""
# df_brand = spark.read.csv("csv_data/Brand.csv")
df_brand = spark.read.option('header', 'true').csv("csv_data/Brand.csv")
df_brand.show()
print("Cantidad de Marcas registradas: " + str(df_brand.count()))

df_category = spark.read.option('header', 'true').csv("csv_data/Category.csv")
df_category.show()
print("Cantidad de Categorias registradas: " + str(df_category.count()))

df_supplier = spark.read.option('header', 'true').csv("csv_data/Supplier.csv")
df_supplier.show()
print("Cantidad de Proveedores registrados: " + str(df_supplier.count()))

"""
Escritura dearchivos .PARQUET
"""

df_brand.write.parquet("parquet_data/Brand.parquet")
df_category.write.parquet("parquet_data/Category.parquet")
df_supplier.write.parquet("parquet_data/Supplier.parquet")