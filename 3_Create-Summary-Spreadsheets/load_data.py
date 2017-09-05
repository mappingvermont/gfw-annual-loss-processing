import os
import pandas as pd
import json
import sqlite3

from utilities import util

conn = util.db_connect()
conn.text_factory = str


def main():

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

    adm_lkp = os.path.join(root_dir, 'data', 'adm_lkp.csv')
    lkp_df = pd.read_csv(adm_lkp)
    lkp_df.loc[lkp_df.adm1 == -9999, 'name1'] = 'N/A'
    lkp_df.loc[lkp_df.adm2 == -9999, 'name2'] = 'N/A'

    lkp_df.loc[lkp_df.name2.isnull(), 'name2'] = 'N/A'

    lkp_df.to_sql('adm_lkp', conn, if_exists='replace')

    for data_type in ['loss', 'extent', 'gain']:
        add_missing_data(lkp_df, data_type)

    print 'inserted'


def json_to_df(json_file):

    with open(json_file) as thefile:
        data = json.load(thefile)['data']

    return pd.DataFrame(data)


def add_missing_data(adm_df, data_type):

    sql = 'SELECT iso, adm1, adm2, sum(area) as area FROM {} GROUP BY iso, adm1, adm2'.format(data_type)        
    df = pd.read_sql(sql, conn)

    join_field_list = ['iso', 'adm1', 'adm2']
    merged = pd.merge(adm_df, df, how='left', on=join_field_list)

    missing_df = merged[merged.area.isnull()]        
    missing_df['area'] = 0.0

    join_field_dict = {'extent': ['thresh'], 'loss': ['thresh', 'year']}

    if data_type in ['extent', 'loss']:
        join_field_list += join_field_dict[data_type]

        # create empty df
        dummy_df = pd.DataFrame()

        for thresh in [0, 10, 15, 20, 25, 30, 50, 75]:
            missing_df['thresh'] = thresh

            if data_type == 'loss':
                 for year in range(2001, 2017):
                     missing_df['year'] = year

                     dummy_df = dummy_df.append(missing_df)

            else:
                dummy_df = dummy_df.append(missing_df)

    else:
        dummy_df = missing_df

    # add area to our output field list
    # then remove all extraneous fields from dataframe
    join_field_list += ['area']
    dummy_df = dummy_df[join_field_list]

    dummy_df.to_sql(data_type, conn, if_exists='append', index=False)

if __name__ == '__main__':
    main()
