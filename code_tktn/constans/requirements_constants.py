date_format = "%Y-%m-%d"
bucket = 'gs://reqs_tktn'
schema_req = {
    "model": "account.move.line",
    "description": "Ventas por almacen de thika thamo",
    "object_version": "1.0.0",
    "model_version": "1.0.0",
    "fields" : [
        {
            "name": "name",
            "type": "str"
        },
        {
            "name": "create_date",
            "type": "datetime"
        },
        {
            "name": "journal_code",
            "type": "str"
        },
        {
            "name": "quantity_signed",
            "type": "float"
        },
        {
            "name": "product_id",
            "type": "list"
        },
        {
            "name": "account_internal_group",
            "type": "str"
        },
        # {
        #     "name": "l10n_pe_edi_warehouse_id",
        #     "type": "list"
        # }
        # {
        #     "name": "move_name",
        #     "type": "str"
        # },
        # {
        #     "name": "team_id",
        #     "type": "list"
        # },
        # {
        #     "name": "product_category_id",
        #     "type": "list"
        # }
    ]
}
