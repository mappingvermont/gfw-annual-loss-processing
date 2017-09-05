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

    output_field_list = join_list + ['name0', 'name1', 'name2', 'area']
    
    # include year + thresh if they exist
    additional_fields = [x for x in ['thresh', 'year'] if x in df.columns]
    output_field_list += additional_fields

    # grab only the fields we want
    merged = merged[output_field_list]
    merged['data_type'] = data_type
    
    output_df = output_df.append(merged)

# reorder fields
#print output_df.columns
output_df = output_df[['iso', 'name0', 'adm1', 'name1', 'adm2', 'name2', 'data_type', 'thresh', 'year', 'area']]
#print output_df.columns

output_df[['thresh', 'year']] = output_df[['thresh', 'year']].fillna(-9999)
output_df[['thresh', 'year']] = output_df[['thresh', 'year']].astype(int)

print output_df.shape

grouped = output_df.groupby(['iso', 'name0', 'adm1', 'name1', 'adm2', 'name2', 'data_type', 'thresh', 'year']).size().reset_index()
print grouped.shape

print grouped[0].unique()
issues = grouped[grouped[0] > 1]
print issues
sys.exit()
    
print 'writing output to CSV'
output_csv = os.path.join('output', 'all_data.csv')
output_df.to_csv(output_csv, index=None, encoding='utf-8')
    
    
