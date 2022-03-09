from controllers.connection_db import connection_odoo_db
from utils.read_schema import read_schema
from utils.read_dates import read_specific_dates
from utils.create_spark_session import create_sparkSession
from utils.create_destination import create_bucket
import pandas as pd

from constans import suppliers_constants

from pyspark.sql.functions import col, explode, expr

"""
explode: crea varias filas segun la cantidad que exista dentro de la lista
concat_ws : elimina las barras, le tienes que pasar el separador y el nombre de la columna
"""
def suppliers_tktn():
    _model = "product.supplierinfo"
    models, db, uid, password = connection_odoo_db()
    suppliers_schema = read_schema(suppliers_constants.schema_suppliers)
    specific_date, next_date = read_specific_dates()

    suppliers = models.execute_kw (
        db,uid,password, _model, 
        'search_read',
        [],
        {
            'fields': suppliers_schema,
            'limit': 30,
            'offset': 0
        }
    )

    
    if len(suppliers) == 0: print("No se encontraron actualizaciones de Proveedores \n"); return suppliers

    sppl = pd.DataFrame(suppliers)
    print(sppl)
    # spark, sc, gcp_connection = create_sparkSession()
    # df_in_suppliers = spark.read.json(sc.parallelize(suppliers))
    # update_date_column = expr("write_date").substr(1,10).alias('update_date')

    # df_in_spark = df_in_suppliers.select(
    #     update_date_column,
    #     "name",
    #     "purchase_order_count"
    # )

    # df_in_spark.show(20, False)

    # if gcp_connection == "True":
    #     # To requeriments of architecture design, this need to be in a specific folder
    #     bucket_specific = create_bucket(suppliers_constants, specific_date)
    #     print("gs: ",bucket_specific)
        
    #     # Write results in GCP
    #     sale_format_parquet = df_in_spark. \
    #     write.mode('append'). \
    #     format("parquet"). \
    #     save(bucket_specific)

    # spark.stop()

    return 0
