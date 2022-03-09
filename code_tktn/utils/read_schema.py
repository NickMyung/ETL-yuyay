def read_schema(object):
    fields = object['fields']
    fields_in_list = []
    for field in fields: 
        name_field = field['name']
        fields_in_list.append(name_field)
    
    return fields_in_list