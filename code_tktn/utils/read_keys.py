import yaml

def read_keys(file: str) -> list:
    odoo_keys = open(file)
    odoo_keys_yaml = yaml.load(odoo_keys, Loader = yaml.FullLoader)
    host = odoo_keys_yaml["host"]
    db = odoo_keys_yaml["db"]
    username = odoo_keys_yaml["username"]
    password = str(odoo_keys_yaml["password"])
    return host, db, username, password