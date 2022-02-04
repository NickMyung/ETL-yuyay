from tokenize import String
from flask import Flask
from flask import request
from flask import jsonify
from flask_cors import CORS
from flask_restful import Api, Resource, reqparse
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

class categorias(Resource):
    def get(self):

        try:                    
            parse = reqparse.RequestParser()
            parse.add_argument('option', type = str)

            args = parse.parse_args()
            option = args['option']
        
            rs,ms = data_get(db, uid, password, models, Option[option].value)
            print(ms)

            DF_to_CSV(rs, Option[option].value['name'])

        except Exception as e:
            print(e)
            rs = []
            ms = "No existe campo: "+ option
            print(ms)
            
        return rs

# Inicializacion API
app = Flask(__name__)
cors = CORS(app)
api = Api(app)

api.add_resource(categorias, '/get/categories')

app.run(debug = True)