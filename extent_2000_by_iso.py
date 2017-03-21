import os
import pandas as pd
import sqlite3
from openpyxl import load_workbook

conn = sqlite3.connect('data.db')


def main():

    root_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(root_dir, 'output')
    output_excel = os.path.join(output_dir, 'tree_cover_stats_2015.xlsx')

    if os.path.exists(output_excel):
        writer = pd.ExcelWriter(output_excel, engine='openpyxl')
        writer.book = load_workbook(output_excel)

    else:
        writer = pd.ExcelWriter(output_excel)

    extent_sql = 'SELECT iso, adm1, adm2, thresh, area FROM extent'
        
    df = pd.read_sql(extent_sql, conn)
    df = add_lookup(df)

    # write iso level
    iso_df = df.copy()
    iso_df.rename(columns={'name0': 'Country'}, inplace=True)

    adm1_df = df.copy()
    adm1_df['Country'] = adm1_df.name0 + '_' + adm1_df.name1

    df_list = [iso_df, adm1_df]
    sheet_type_list = ['Country', 'Subnat']

    for to_write_df, sheet_type in zip(df_list, sheet_type_list):

        # Group by Country and thresh, sum area
        to_write_df = to_write_df.groupby(['Country', 'thresh'])['area'].sum().reset_index()

        # pivot and remove column where thresh == 0
        df_pivot = to_write_df.pivot(index='Country', columns='thresh', values='area')
        del df_pivot[0]

        # rename columns to match output
        df_pivot.columns = ['>10%', '>15%', '>20%', '>25%', '>30%', '>50%', '>75%']

        df_pivot.to_excel(writer, 'Extent (2000) by {}'.format(sheet_type))

    writer.save()


def add_lookup(data_df):

    lkp_df = pd.read_sql('SELECT iso, adm1, adm2, name0, name1, name2 FROM adm_lkp', conn)

    data_df = pd.merge(data_df, lkp_df, on=['iso', 'adm1', 'adm2'], how='left')

    return data_df
    
if __name__ == '__main__':
    main()
