from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys

def create_sparkSession() -> tuple:
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("Ventas dashboard") \
        .getOrCreate()
    

    gcp_connection = "True"

    try:
        gcp_connection = sys.argv[2]
        argv_2 = "exists"
        print("Parametro de conexión: "+ str(gcp_connection))
    except:
        argv_2 = None
        print("No se envio parámetro de Conexión")
        pass

    #CADA UNO TENDRA UN USUARIO
    if gcp_connection == "True" and argv_2 != None:
        spark.conf.set("fs.project.id", "yuyaytest")
        spark.conf.set("google.cloud.auth.service.account.enable", "true")
        spark.conf.set("google.cloud.auth.service.account.email", "nicksc1911@yuyaytest.iam.gserviceaccount.com")
        spark.conf.set("google.cloud.auth.service.account.keyfile", "credentials/yuyaytest-e4234bace7dd.p12")
    
    """
    Para conectar a Cloud Storage, puedes hacerlo mediante la configuracion(ahora)
    o en los mismos archivos de hadoop cambiar la configuracion https://stackoverflow.com/questions/55595263/how-to-fix-no-filesystem-for-scheme-gs-in-pyspark
    """
    
    return spark, spark.sparkContext, gcp_connection 
