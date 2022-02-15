#import pyarrow as pa
#import pyarrow.parquet as pq
import pandas as pd

def DF_to_CSV(rs, name):
    df = pd.DataFrame(rs)
    print()
    print(df.head(5))
    print('Numero de Filas: '+str(len(df)))
    print()

    df.to_csv('csv_data/'+str(name)+'.csv')
    pass


