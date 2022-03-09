from controllers.connection_db import connection_odoo_db
from utils.read_schema import read_schema
from utils.read_dates import read_specific_dates
from utils.create_spark_session import create_sparkSession
from utils.create_destination import create_bucket

from constans import products_constants

from pyspark.sql.functions import col, explode, expr, udf, split, length

"""
explode: crea varias filas segun la cantidad que exista dentro de la lista
concat_ws : elimina las barras, le tienes que pasar el separador y el nombre de la columna
"""
def products_tktn():
    _model = "product.product"
    models, db, uid, password = connection_odoo_db()
    products_schema = read_schema(products_constants.schema_products)
    specific_date, next_date = read_specific_dates()

    domains_products = [
            ['write_date', '>=', specific_date],
            ['write_date', "<", next_date]
    ]
    products = models.execute_kw (
        db,uid,password, _model, 
        'search_read',
        [domains_products],
        {
            'fields': products_schema,
            'limit': 10,
            'offset': 0
        }
    )

    if len(products) == 0: print("No se encontraron actualizaciones de Productos \n"); return products
    print(products)
    spark, sc, gcp_connection = create_sparkSession()
    df_in_products = spark.read.json(sc.parallelize(products))
    print(df_in_products)

    df_in_products = df_in_products.withColumn("update_hour", expr("write_date")). \
                                    withColumn("Sub_category_name", split(expr("categ_id[1]"), "/").getItem(1)). \
                                    withColumn("category_name", split(expr("categ_id[1]"), "/").getItem(0))

    update_hour_column = expr("update_hour").substr(11,20).alias('update_hour')
    update_date_column = expr("write_date").substr(1,10).alias('update_date')
    warehouse_name_column = expr('warehouse_list[0]["warehouse_id"][1]').alias('warehouse_name')
    category_name_column = expr("category_name").alias('category_name')
    sub_category_name_column = expr("Sub_category_name").substr(2,100).alias('Sub_category_name')
    supplier_name_column = expr("seller_ids").alias("supplier_id")
    quantity_name_column = expr("qty_available").alias("quantity_available")
    standard_price_column = expr("standard_price").substr(1,6).alias("standard_price")
    list_price_column = expr("list_price").substr(1,4).alias("list_price")

    df_in_spark = df_in_products.select(
        update_hour_column,
        update_date_column,
        "name",
        warehouse_name_column,
        category_name_column,
        sub_category_name_column,
        supplier_name_column,
        standard_price_column,
        list_price_column,
        "brand_name",
        quantity_name_column
    )

    df_in_spark.show(20, False)
    
    if gcp_connection == "True":
        # To requeriments of architecture design, this need to be in a specific folder
        bucket_specific = create_bucket(products_constants, specific_date)
        print("gs: ",bucket_specific)
        
        # Write results in GCP
        sale_format_parquet = df_in_spark. \
        write.mode('append'). \
        format("parquet"). \
        save(bucket_specific)

    spark.stop()

    return 0
