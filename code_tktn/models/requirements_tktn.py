from controllers.connection_db import connection_odoo_db
from utils.read_schema import read_schema
from utils.read_dates import read_specific_dates
from utils.create_spark_session import create_sparkSession
from utils.create_destination import create_bucket
from utils.df_processing import replaceFalse
from utils.df_processing import warehouseByjournal, data_concat 
import pandas as pd
#from IPython.display import display

from constans import requirements_constants

from pyspark.sql.functions import col, concat_ws, explode, expr, split, concat_ws, UserDefinedFunction
from pyspark.sql.types import StringType

"""
explode: crea varias filas segun la cantidad que exista dentro de la lista
concat_ws : elimina las barras, le tienes que pasar el separador y el nombre de la columna
"""
def requirements_tktn():

    """
    #####################################################
                    ██████████████████████
                    █─▄▄─█▄─▄▄▀█─▄▄─█─▄▄─█  
                    █─██─██─██─█─██─█─██─█
                    ▀▄▄▄▄▀▄▄▄▄▀▀▄▄▄▄▀▄▄▄▄▀
    #####################################################
    """
    _model = "account.move.line"
    models, db, uid, password = connection_odoo_db()
    req_schema = read_schema(requirements_constants.schema_req)
    specific_date, next_date = read_specific_dates()
    print("Date: ")
    print(specific_date)
    domains_reqs = [
            ['create_date', '>=', specific_date],
            ['create_date', "<", next_date],
            ['journal_code','!=',"STJ"], 
            ['journal_code','!=',"PSBC2"],
            ['journal_code','!=',"PSBC1"], 
            ['journal_code','!=',"CJSB"],
            ['journal_code','!=',"CSH1"],
            ['journal_code','!=',"PMFSH"],
            ['journal_code','!=',"PMFC1"],
            ['journal_code','!=',"PMFC2"],
            ['journal_code','!=',"PSBSH"],
            ['journal_code','!=',"PMFON"],
            ['journal_code','!=',"POSPL"],
            ['journal_code','!=',"CSHpL"],
            ['journal_code','!=',"LNiub"],
            ['journal_code','!=',"PRSV"],
            ['journal_code','!=',"CJSH"],
            ['journal_code','!=',"TNIUB"],
            ['journal_code','!=',"PSBON"],
            ['journal_code','!=',"INV"],
            ['journal_code','!=',"TLCDT"],
            ['journal_code','!=',"MISC"],
            ['journal_code','!=',"PLNLL"],
            ['journal_code','!=',"BNKI"],
            ['journal_code','!=',"PRSV"],
            ['journal_code','!=',"NC01"],
            ['journal_code','!=',"N010"],
            ['journal_code','!=',"POSS"],
            ["account_internal_group",'=','income']
            ]

    reqs = models.execute_kw (
        db,uid,password, _model, 
        'search_read',
        [domains_reqs],
        {
            'fields': req_schema,
            #'limit': 3,
            'offset': 0
        }
    )
    print("Cantidad de datos extraidos: {} \n".format(str(len(reqs))))

    for i in range(len(reqs)):
        try:
            prod = models.execute_kw(
            db,uid,password, "product.product", 
            'search_read',
            [[["id","=",reqs[i]["product_id"][0]]]],
            {
                'fields': ["brand_name", "seller_ids"],
                'offset': 0
            }
            )
            sppls_prod = []
            try:
                for id in prod[0]["seller_ids"]:
                    sppl = models.execute_kw(
                        db,uid,password, "product.supplierinfo", 
                        'search_read',
                        [[['id','=',id]]],
                        {
                            'fields': ["name"],
                            'offset': 0
                        }
                    )
                    sppls_prod.append(sppl[0]["name"][1])
            except Exception as e:
                print("Llego: "+ str(e))           
                
            reqs[i]["Diferencia"] = ""
            reqs[i]["Columna_1"] = ""
            if(len(prod)>0):
                reqs[i]["brand_name"] = prod[0]["brand_name"]
                reqs[i]["seller_ids"] = sppls_prod[0]
            else:
                reqs[i]["brand_name"] = ""
                reqs[i]["seller_ids"] = ""

        except Exception as e:
            print("Salto error:" + str(e))
            print("Prod: {}--{}\n".format(prod,i))
            reqs[i]["Diferencia"] = ""
            reqs[i]["Columna_1"] = ""
            reqs[i]["brand_name"] = ""
            reqs[i]["seller_ids"] = ""
            

    reqs = replaceFalse(pd.DataFrame(reqs))
    #print(reqs)
    
    if len(reqs) == 0: print("No se encontraron actualizaciones de Ventas \n"); return reqs    

    """
    #####################################################
         ███████████████████████████████████████████
         █▄─▄▄─█▄─█─▄█─▄▄▄▄█▄─▄▄─██▀▄─██▄─▄▄▀█▄─█─▄█
         ██─▄▄▄██▄─▄██▄▄▄▄─██─▄▄▄██─▀─███─▄─▄██─▄▀██
         ▀▄▄▄▀▀▀▀▄▄▄▀▀▄▄▄▄▄▀▄▄▄▀▀▀▄▄▀▄▄▀▄▄▀▄▄▀▄▄▀▄▄▀
    #####################################################
    """
    spark, sc, gcp_connection = create_sparkSession()
    df_in_req = spark.read.json(sc.parallelize(list(reqs.T.to_dict().values())))
    #df_in_req.show(200, False)

    udf_wh = UserDefinedFunction(lambda x: warehouseByjournal(x), StringType())
    udf_con = UserDefinedFunction(lambda x: data_concat(x), StringType())

    df_in_req = df_in_req.withColumn("create_hour", expr("create_date")).\
                          withColumn("warehouse_id",udf_wh(expr("journal_code"))) .\
                          withColumn("brand_name",udf_con(expr("brand_name"))).\
                          withColumn("seller_ids",udf_con(expr("seller_ids")))    

    supplier_name_column = expr("seller_ids").alias("Proveedor")
    warehouse_name_column = expr("warehouse_id").alias("Establecimiento")
    brand_name_column = expr("brand_name").alias("Marca")
    product_name_column = expr("name").alias("Producto")
    journal_name_column = expr("journal_code").alias("Serie_de_Comprobante")
    create_hour_column = expr("create_hour").substr(12,20).alias('Hora')
    create_date_column = expr("create_date").substr(1,10).alias('Fecha')
    quantity_name_column = expr("quantity_signed").alias("Cantidad")

    df_in_spark = df_in_req.select(
        "Columna_1",
        supplier_name_column,
        brand_name_column,
        product_name_column,
        warehouse_name_column,
        journal_name_column,
        "Diferencia",
        create_hour_column,
        create_date_column,
        quantity_name_column,
    )

    df_in_spark.show(200, False)

    if gcp_connection == "True":
        # To requeriments of architecture design, this need to be in a specific folder
        bucket_specific = create_bucket(requirements_constants, specific_date)
        print("gs: ",bucket_specific)
        
        # Write results in GCP
        sale_format_parquet = df_in_spark. \
        write.mode('append'). \
        format("parquet"). \
        save(bucket_specific)    

    spark.stop()

    return 0