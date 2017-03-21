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
    
    extent_sql = 'SELECT iso, adm1, thresh, year, sum(area) as area FROM loss ' \
                 'GROUP BY iso, adm1, thresh, year'

    df = pd.read_sql(extent_sql, conn)
    print df
    print 'got data'
    df = add_lookup(df)
    print 'added lookup'

    skip_list = ['AFG', 'ARE', 'CHN', 'ESP', 'IRN', 'IRQ', 'ISR', 'ITA', 'JOR', 'JPN',
                 'KGZ', 'KWT', 'OMN', 'PSE', 'SAU', 'TJK', 'TKM', 'USA', 'ALB', 'CYP',
                 'GRC', 'LBN', 'MUS', 'PRK', 'XNC', 'PRT', 'KOR', 'SYR', 'TUR', 'UZB']

    # Create country field name
    df['Country'] = df.name0 + '_' + df.name1

    print df
    print df.columns

    thresh10_df = df[df.thresh == 10]
    thresh10_pivot = thresh10_df.pivot(index='Country', columns='year', values='area')

    for thresh in [15, 20, 25, 30, 50, 75]:
        df_subset = df[df.thresh == thresh]
        print df_subset

        df_pivot = df_subset.pivot(index='Country', columns='year', values='area')
        thresh10_pivot = pd.merge(thresh10_pivot, df_pivot, how='left', on='Country')

    print thresh10_pivot

    thresh10_df.to_excel(writer, 'Loss (2001-2014) by Subnat', index=False)

    writer.save()


def add_lookup(data_df):

    lkp_df = pd.read_sql('SELECT iso, adm1, adm2, name0, name1, name2 FROM adm_lkp', conn)
    lkp_df = lkp_df.groupby(['iso', 'adm1', 'name0', 'name1']).size().reset_index()

    print 'got lkp df'

    del lkp_df[0]

    data_df = pd.merge(data_df, lkp_df, on=['iso', 'adm1'], how='left')

    return data_df
    
if __name__ == '__main__':
    main()
