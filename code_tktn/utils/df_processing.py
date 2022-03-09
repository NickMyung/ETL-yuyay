from typing import final
from constans.warehouseByjournal import warehouse_journals

def replaceFalse(dataFrame):
    import pandas as pd
    dataFrame = dataFrame.replace(False," ")
    return dataFrame


def warehouseByjournal(journal_code):
    warehouse = ""
    for (wh, journals) in warehouse_journals.items():
        for journal in journals:
            if str(journal_code)[1:] == str(journal):
                warehouse = wh
                break
    if warehouse == "":
        print("Error-Unknown Journal Code: " + str(journal_code))
    return "Thika_Thani_" + warehouse

def data_concat(data):
    final_data = data
    if len(data.split(sep = "-"))>1:
        data = " "+data
        split_data = data.split(sep = "-")
        final_data = ""
        for wd in split_data:
            final_data = final_data + wd[1:]
        final_data = final_data
    if len(data.split(sep = "."))>1: 
        split_data = final_data.split(sep = ".")
        final_data = ""
        for wd in split_data:
            final_data = final_data + wd
        final_data = final_data
    if len(data.split(sep = " "))>1:
        split_data = final_data.split(sep = " ")
        final_data = ""
        for wd in split_data:
            final_data = final_data + "_"+wd
        final_data = final_data[1:]

    return final_data
    
