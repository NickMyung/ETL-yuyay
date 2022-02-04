from search import *
from search_data import *
from conversion import *

"""
Autenticacion Odoo
"""
url = 'https://yuyay-yuyaytest-3202252.dev.odoo.com'
db = 'yuyay-yuyaytest-3202252'
username = 'kasway@thikathani.com.pe'
password = '12345678'
import xmlrpc.client

"""
Inicializacion de Modelo
"""
common = xmlrpc.client.ServerProxy('%s/xmlrpc/2/common' % url)
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

def query(option):
    try:                    
   
        rs,ms = data_get(db, uid, password, models, Option[option].value)
        print(ms)

        DF_to_CSV(rs, Option[option].value['name'])

    except Exception as e:
        print(e)
        rs = []
        ms = "No existe campo: "+ option
        print(ms)
        
    return rs

query('brand')
print()
query('category')
print()
query('supplier')