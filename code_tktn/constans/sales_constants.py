date_format = "%Y-%m-%d"
bucket = 'gs://sales_tktn'
schema_sales = {
    "model": "sale.order",
    "description": "Ventas por almacen de thika thamo",
    "object_version": "1.0.0",
    "model_version": "1.0.0",
    "fields" : [
        {
            "name": "name",
            "type": "str"
        },
        {
            "name": "partner_id",
            "type": "list"
        },
        {
            "name": "date_order",
            "type": "datetime"
        },
        {
            "name": "warehouse_id",
            "type": "int" 
        },
        {
            "name": "team_id",
            "type": "str"
        },
        {
            "name": "l10n_latam_document_type_id",
            "type": "str"
        }
    ]
}
