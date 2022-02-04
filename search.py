def data_get(db, uid, password, models, option):
   
    ps = models.execute_kw(db, uid, password,
        option['object'], 'search_read',[], # [[['purchase_order_count', '>', 0]]]
        {'fields':option['field']})

    if option['name'] == 'Supplier':                        
        rs = []
        for i in range(len(ps)):
            if ps[i]['purchase_order_count'] > 0:
                rs.append(ps[i])
    else: rs = ps             
    ms = "\nSe obtuvo "+str(len(rs))+" resultados de campo: " + option['name'] 

    return rs, ms