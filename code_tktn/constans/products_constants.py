date_format = "%Y-%m-%d"
bucket = 'gs://products_tktn'
schema_products = {
    "model": "product.product",
    "description": "Productos registradaos en thika thani",
    "object_version": "1.0.0",
    "model_version": "1.0.0",
    "fields" : [
        {
            "name":"write_date",
            "type":"datetime"
        },
        {
            "name": "name",
            "type": "str"
        },
        {
            "name": "warehouse_list",
            "type": "list"
        },
        {
            "name": "categ_id",
            "type": "list" 
        },
        {
            "name": "brand_name",
            "type": "str"
        },
        {
            "name": "seller_ids",
            "type": "list"
        },
        {
            "name": "standard_price",
            "type": "float"
        },
        {
            "name": "list_price",
            "type": "float"
        },
        {
            "name": "qty_available",
            "type": "int"
        }   
    ]
}
