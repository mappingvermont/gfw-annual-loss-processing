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

    extent2000_src = os.path.join(src_dir, 'extent2000.csv')
    extent2010_src = os.path.join(src_dir, 'extent2010.csv')
    loss_src = os.path.join(src_dir, 'loss.csv')
    gain_src = os.path.join(src_dir, 'gain.csv')

    extent2000_df = filter_csv(extent2000_src)
    extent2010_df = filter_csv(extent2010_src)
    loss_df = filter_csv(loss_src)
    gain_df = filter_csv(gain_src)

    table_list = ['extent2000', 'extent2010', 'loss', 'gain']
    df_list = [extent2000_df, extent2010_df, loss_df, gain_df]

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

    for data_table in ['loss', 'extent2000', 'extent2010', 'gain']:
        add_missing_data(lkp_df, data_table)

    print 'inserted'


def filter_csv(input_csv):

    print 'reading {}'.format(input_csv)

    # read in df, filtering for only gadm28
    # don't care about any of the other datasets used by country pages
    df = pd.read_csv(input_csv)
    df = df[df.polyname == 'gadm28_large'].copy()

    skip_col_list = ['polyname', 'bound1', 'bound2', 'bound3', 'bound4', 'emissions']
    col_list = [x for x in df.columns.tolist() if x not in skip_col_list]

    # remove unnecessary columns
    df = df[col_list]

    return df


def add_missing_data(adm_df, data_table):

    # find existing iso/adm1/adm2 combinations in the data table of interest
    sql = 'SELECT iso, adm1, adm2, sum(area) as area FROM {} GROUP BY iso, adm1, adm2'.format(data_table)        
    df = pd.read_sql(sql, conn)

    # join them to the table of all iso/adm1/adm2
    join_field_list = ['iso', 'adm1', 'adm2']
    merged = pd.merge(adm_df, df, how='left', on=join_field_list)

    # filter to get a list of iso/adm1/adm2 rows without any data in the source table
    # source in this case being loss/extent2000/extent2010/gain
    missing_df = merged[merged.area.isnull()]        
    missing_df['area'] = 0.0

    join_field_dict = {'extent2000': ['thresh'], 'extent2010': ['thresh'], 'loss': ['thresh', 'year']}

    if data_table in ['extent2000', 'extent2010', 'loss']:
        join_field_list += join_field_dict[data_table]

        # create empty df
        dummy_df = pd.DataFrame()

        # will need a thresh value for each missing iso/adm1/adm2 row
        for thresh in [0, 10, 15, 20, 25, 30, 50, 75]:
            missing_df['thresh'] = thresh

            # and if the input is loss, will need a year value as well
            if data_table == 'loss':
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

    # after we've built our dummy table, append it to the source table in sql
    # this will add all the dummy rows, ensuring we have values for every iso/adm1/adm2 row possible
    dummy_df.to_sql(data_table, conn, if_exists='append', index=False)

if __name__ == '__main__':
    main()

