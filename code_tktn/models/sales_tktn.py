from controllers.connection_db import connection_odoo_db
from utils.read_schema import read_schema
from utils.read_dates import read_specific_dates
from utils.create_spark_session import create_sparkSession
from utils.create_destination import create_bucket

from constans import sales_constants

from pyspark.sql.functions import col, explode, expr

"""
explode: crea varias filas segun la cantidad que exista dentro de la lista
concat_ws : elimina las barras, le tienes que pasar el separador y el nombre de la columna
"""
def sales_tktn():
    _model = "sale.order"
    models, db, uid, password = connection_odoo_db()
    sales_schema = read_schema(sales_constants.schema_sales)
    specific_date, next_date = read_specific_dates()

    domains_sales = [
            ['date_order', '>=', specific_date],
            ['date_order', "<", next_date]
    ]
    sales = models.execute_kw (
        db,uid,password, _model, 
        'search_read',
        [domains_sales],
        {
            'fields': sales_schema,
            'limit': 10,
            'offset': 0
        }
    )

    if len(sales) == 0: print("No se encontraron actualizaciones de Ventas \n"); return sales

    spark, sc, gcp_connection = create_sparkSession()
    df_in_sales = spark.read.json(sc.parallelize(sales))
    date_order_column = expr("date_order").substr(1,10).alias('date_order')
    partner_name_column = expr('partner_id[1]').alias('partner_name')
    warehouse_name_column = expr('warehouse_id[1]').alias('warehouse_name')

    df_in_spark = df_in_sales.select(
        date_order_column,
        'name',
        partner_name_column,
        warehouse_name_column,
        'l10n_latam_document_type_id'
    )
    df_in_spark.show(20, False)
    
    if gcp_connection == "True":
        # To requeriments of architecture design, this need to be in a specific folder
        bucket_specific = create_bucket(sales_constants, specific_date)
        print("gs: ",bucket_specific)
        # Write results in GCP
        sale_format_parquet = df_in_spark. \
        write.mode('append'). \
        format("parquet"). \
        save(bucket_specific)        

    spark.stop()

    return 0