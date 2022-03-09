import xmlrpc.client

def connection_odoo_db():
    host = "https://yuyay.odoo.com" # https://yuyay-yuyaytest-3202252.dev.odoo.com
    db = "abrazandoalperu-main-1755588"   # yuyay-yuyaytest-3202252
    username = "kasway@thikathani.com.pe"
    password = "12345678"  # ce177617b8c3a714acdbe38d81a44a5531eed57c

    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(host), allow_none=True)
    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(host))
    uid = common.authenticate(db, username, password, {})
    return models,db,uid,password
