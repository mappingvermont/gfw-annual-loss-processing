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
    df = add_lookup(df)

    # Create country field name
    df['Country'] = df.name0 + '_' + df.name1

    is_first = True

    for thresh in [10, 15, 20, 25, 30, 50, 75]:
        df_subset = df[df.thresh == thresh]

        df_pivot = df_subset.pivot(index='Country', columns='year', values='area')
        df_pivot['TOTAL 2001-2015'] = df_pivot.sum(axis=1)

        if is_first:
            output_df = df_pivot.copy()
            is_first = False

        else:
            output_df = pd.concat([output_df, df_pivot], axis=1, join_axes=[output_df.index])

    output_df.to_excel(writer, 'Loss (2001-2015) by Subnat')

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
