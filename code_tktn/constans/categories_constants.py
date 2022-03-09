date_format = "%Y-%m-%d"
bucket = 'gs://categories_tktn'
schema_categories = {
   "model": "product.category",
    "description": "Categorias registradas en thika thani",
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
            "name": "internal_code",
            "type": "int"
        },
        {
            "name": "product_count",
            "type": "int" 
        }
    ]
}