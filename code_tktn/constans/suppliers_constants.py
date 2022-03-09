date_format = "%Y-%m-%d"
bucket = 'gs://suppliers_tktn'
schema_suppliers = {
    "model": "res.partner",
    "description": "Proveedores registrados en thika thani",
    "object_version": "1.0.0",
    "model_version": "1.0.0",
    "fields" : [
        {
            "name": "name",
            "type": "str"
        },
        {
            "name": "product_name",
            "type": "str" 
        },
        {
            "name": "product_id",
            "type": "list" 
        },
        {
            "name": "product_tmpl_id",
            "type": "list"
        }
    ]
}
