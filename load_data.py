import os
import pandas as pd
import json
import sqlite3


def main():
    conn = sqlite3.connect('data.db')
    conn.text_factory = str
    
    root_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.join(root_dir, 'source')
    
    extent2000_src = os.path.join(src_dir, 'extent2000.json')
    loss_src = os.path.join(src_dir, 'loss.json')
    gain_src = os.path.join(src_dir, 'gain.csv')
    
    extent2000_df = json_to_df(extent2000_src)
    loss_df = json_to_df(loss_src)
    gain_df = pd.read_csv(gain_src, header=None, names=['iso', 'adm1', 'adm2', 'area'])
    
    table_list = ['extent', 'loss', 'gain']
    df_list = [extent2000_df, loss_df, gain_df]
    
    for table_name, df in zip(table_list, df_list):
        print 'inserting into {}'.format(table_name)
        
        df.to_sql(table_name, conn, if_exists='replace')

        del df
        
    lkp_df = pd.read_csv('adm_lkp.csv')
    lkp_df.loc[lkp_df.adm1 == -9999, 'name1'] = 'N/A'
    lkp_df.loc[lkp_df.adm2 == -9999, 'name2'] = 'N/A'
    
    lkp_df.to_sql('adm_lkp', conn, if_exists='replace')
    
    print 'inserted'

    
def json_to_df(json_file):

    with open(json_file) as thefile:
        data = json.load(thefile)['data']
        
    return pd.DataFrame(data)
    
    
if __name__ == '__main__':
    main()
