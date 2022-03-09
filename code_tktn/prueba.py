import xmlrpc.client
import pandas as pd
from utils.df_processing import warehouseByjournal, data_concat 


# PRODUCCION
host = "https://yuyay.odoo.com"
db = "abrazandoalperu-main-1755588"
user = "kasway@thikathani.com.pe"
password = "12345678"

common = xmlrpc.client.ServerProxy(
    '{}/xmlrpc/2/common'.format(host))
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(
    '{}/xmlrpc/2/object'.format(host))


try:

    # move_ides= models.execute_kw(db, uid, password, 'account.move.line', 'search_read', [[
    #                 ['journal_code','!=',"STJ"], 
    #         ['journal_code','!=',"PSBC2"],
    #         ['journal_code','!=',"PSBC1"], 
    #         ['journal_code','!=',"CJSB"],
    #         ['journal_code','!=',"CSH1"],
    #         ['journal_code','!=',"PMFSH"],
    #         ['journal_code','!=',"PMFC1"],
    #         ['journal_code','!=',"PMFC2"],
    #         ['journal_code','!=',"PSBSH"],
    #         ['journal_code','!=',"PMFON"],
    #         ['journal_code','!=',"POSPL"],
    #         ['journal_code','!=',"CSHpL"],
    #         ['journal_code','!=',"LNiub"],
    #         ['journal_code','!=',"PRSV"],
    #         ['journal_code','!=',"CJSH"],
    #         ['journal_code','!=',"TNIUB"],
    #         ['journal_code','!=',"PSBON"],
    #         ['journal_code','!=',"INV"],
    #         ['journal_code','!=',"TLCDT"],
    #         ['journal_code','!=',"MISC"],
    #         ['journal_code','!=',"PLNLL"],
    #         ['journal_code','!=',"BNKI"],
    #         ['journal_code','!=',"PRSV"],
    #         ['journal_code','!=',"NC01"],
    #         ['journal_code','!=',"N010"],
    #         ['journal_code','!=',"POSS"],
    #         ]],  {
    #                 'fields':[], 'limit':2
    #             }
    #             )
    a = models.execute_kw(db, uid, password, 'res.partner', 'search_read', [[['name','=','EMPRESA NACIONAL DE CACAO S.A.C.']]],{
                    'fields':['id', 'product_supplierinfo_ids', 'name'], 'limit':5    
                }
                )
    b = models.execute_kw(db, uid, password, 'product.template', 'search_read', [[['brand_name','=','ALMANZOR']]],{
                    'fields':['id', 'seller_ids', 'name'], 'limit':5    
                }
                )
    c = models.execute_kw(db, uid, password, 'product.supplierinfo', 'search_read', [[['id','=','4444']]],{
                    'fields':['id', 'name'], 'limit':5    
                }
                )
    d = models.execute_kw(db, uid, password, 'product.template', 'search_read', [[['id','=','74912']]],{
                    'fields':['id', 'seller_ids', 'name','brand_name'], 'limit':5    
                }
                )         
      

    #print(pd.DataFrame(move_ides)['journal_code'].unique())
    #print(pd.DataFrame(move_ides)) 
    print(a)
    print(b)
    print(c)
    print(d)
except Exception as error:
    e=str(error)
    print(e)

print(data_concat("NUTRILIS S.A.C. - 20600752180"))