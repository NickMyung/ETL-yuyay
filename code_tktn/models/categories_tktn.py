from controllers.connection_db import connection_odoo_db
from models.brands_tktn import brands_tktn
from utils.read_schema import read_schema
from utils.read_dates import read_specific_dates
from utils.create_spark_session import create_sparkSession
from utils.create_destination import create_bucket

from constans import categories_constants

from pyspark.sql.functions import col, explode, expr

"""
explode: crea varias filas segun la cantidad que exista dentro de la lista
concat_ws : elimina las barras, le tienes que pasar el separador y el nombre de la columna
"""
def categories_tktn():
    _model = "product.category"
    models, db, uid, password = connection_odoo_db()
    categories_schema = read_schema(categories_constants.schema_categories)
    specific_date, next_date = read_specific_dates()

    domains_categories = [
            ['write_date', '>=', specific_date],
            ['write_date', "<", next_date]
    ]
    categories = models.execute_kw (
        db,uid,password, _model, 
        'search_read',
        [domains_categories],
        {
            'fields': categories_schema,
            'limit': 10,
            'offset': 0
        }
    )

    if len(categories) == 0: print("No se encontraron actualizaciones de Categorias \n"); return categories

    spark, sc, gcp_connection = create_sparkSession()
    df_in_categories = spark.read.json(sc.parallelize(categories))
    update_date_column = expr("write_date").substr(1,10).alias('update_date')

    df_in_spark = df_in_categories.select(
        update_date_column,
        "name",
        "product_count"
    )

    df_in_spark.show(20, False)
    
    if gcp_connection == "True":
        # To requeriments of architecture design, this need to be in a specific folder
        bucket_specific = create_bucket(categories_constants, specific_date)
        print("gs: ",bucket_specific)
        
        # Write results in GCP
        sale_format_parquet = df_in_spark. \
        write.mode('append'). \
        format("parquet"). \
        save(bucket_specific) 

    spark.stop()

    return 0
