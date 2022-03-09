from controllers.connection_db import connection_odoo_db
from utils.read_schema import read_schema
from utils.read_dates import read_specific_dates
from utils.create_spark_session import create_sparkSession
from utils.create_destination import create_bucket

from constans import brands_constants

from pyspark.sql.functions import col, explode, expr

"""
explode: crea varias filas segun la cantidad que exista dentro de la lista
concat_ws : elimina las barras, le tienes que pasar el separador y el nombre de la columna
"""
def brands_tktn():
    _model = "product.brand"
    models, db, uid, password = connection_odoo_db()
    brands_schema = read_schema(brands_constants.schema_brands)
    specific_date, next_date = read_specific_dates()

    domains_brands = [
            ['write_date', '>=', specific_date],
            ['write_date', "<", next_date]
    ]
    brands = models.execute_kw (
        db,uid,password, _model, 
        'search_read',
        [domains_brands],
        {
            'fields': brands_schema,
            'limit': 10,
            'offset': 0
        }
    )

    if len(brands) == 0: print("No se encontraron actualizaciones de Marcas \n"); return brands

    spark, sc, gcp_connextion = create_sparkSession()
    df_in_brands = spark.read.json(sc.parallelize(brands))
    update_date_column = expr("write_date").substr(1,10).alias('update_date')

    df_in_spark = df_in_brands.select(
        update_date_column,
        "name",
        "product_count"
    )

    df_in_spark.show(20, False)

    if gcp_connextion == "True":
        # To requeriments of architecture design, this need to be in a specific folder
        bucket_specific = create_bucket(brands_constants, specific_date)
        print("gs: ",bucket_specific)
        
        # Write results in GCP
        sale_format_parquet = df_in_spark. \
        write.mode('append'). \
        format("parquet"). \
        save(bucket_specific)

    spark.stop()

    return 0
