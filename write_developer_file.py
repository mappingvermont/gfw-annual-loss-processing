import os
import pandas as pd


from utilities import util

conn = util.db_connect()

adm_sql = 'SELECT * FROM adm_lkp'
adm_df = pd.read_sql(adm_sql, conn)

output_df = pd.DataFrame()

for data_type in ['loss', 'extent', 'gain']:
    sql = 'SELECT * FROM {}'.format(data_type)
    df = pd.read_sql(sql, conn)
    
    df.area = df.area / 10000
    
    join_list = ['iso', 'adm1', 'adm2']
    merged = pd.merge(adm_df, df, on=join_list)
    
    print data_type
    qc = merged.groupby(['iso', 'adm1', 'adm2']).size().reset_index()
    print qc.shape
    
    print merged.shape
    output_field_list = join_list + ['name0', 'name1', 'name2']
    
    # include year + thresh if they exist
    additional_fields = [x for x in ['thresh', 'year'] if x in df.columns]
    output_field_list += additional_fields
    
    # grab only the fields we want
    merged = merged[output_field_list]
    merged['data_type'] = data_type
    
    output_df = output_df.append(merged)
    
print 'writing output to CSV'
output_csv = os.path.join('output', 'all_data.csv')
output_df.to_csv(output_csv, index=None, encoding='utf-8')
    
    