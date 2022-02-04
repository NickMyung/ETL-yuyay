from enum import Enum

class Option(Enum):
    category = {'name':'Category','object':'product.category', 'field':['name', 'product_count']} 
    brand = {'name':'Brand','object':'product.brand', 'field':['internal_code','name', 'product_count']}
    supplier = {'name':'Supplier','object':'res.partner', 'field':["name", "purchase_order_count"]}
