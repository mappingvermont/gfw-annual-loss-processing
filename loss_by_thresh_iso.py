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
    
    extent_sql = 'SELECT iso, thresh, year, sum(area) as area FROM loss ' \
                 'GROUP BY iso, thresh, year'

    df = pd.read_sql(extent_sql, conn)
    df = add_lookup(df)

    # Create country field name
    df['Country'] = df.name0

    # remove partial country data
    partial_list = ['AFG', 'ARE', 'CHN', 'ESP', 'IRN', 'IRQ', 'ISR', 'ITA', 'JOR', 'JPN',
                 'KGZ', 'KWT', 'OMN', 'PSE', 'SAU', 'TJK', 'TKM', 'USA', 'ALB', 'CYP',
                 'GRC', 'LBN', 'MUS', 'PRK', 'XNC', 'PRT', 'KOR', 'SYR', 'TUR', 'UZB']

    complete_miss_list = ['ALA', 'ASM', 'AND', 'ATA', 'ARM', 'AUT', 'AZE', 'BLR', 'BEL', 'BMU', 
                         'BIH', 'BVT', 'IOT', 'BGR', 'CAN', 'XCA', 'CXR', 'CCK', 'COK', 'HRV', 
                         'CZE', 'DNK', 'EST', 'FRO', 'FIN', 'FRA', 'PYF', 'GEO', 'DEU', 'GRL', 
                         'GUM', 'GGY', 'HMD', 'HUN', 'ISL', 'IRL', 'IMN', 'JEY', 'KAZ', 'XKO', 
                         'LVA', 'LIE', 'LTU', 'LUX', 'MKD', 'MHL', 'MDA', 'MCO', 'MNG', 'MNE', 
                         'NLD', 'NIU', 'MNP', 'NOR', 'PCN', 'POL', 'ROU', 'RUS', 'SHN', 'SPM', 
                         'WSM', 'SMR', 'SRB', 'SVK', 'SVN', 'SGS', 'SJM', 'SWE', 'CHE', 'TKL', 
                         'TON', 'UKR', 'GBR', 'VAT', 'WLF']

    skip_list = partial_list + complete_miss_list

    # set area = -9999 where we have partial country data
    df.loc[((df.iso.isin(skip_list)) & (df.year == 2015)), 'area'] = -9999

    thresh10_df = df[df.thresh == 10]
    thresh10_pivot = thresh10_df.pivot(index='Country', columns='year', values='area')

    print thresh10_pivot.columns

    thresh10_pivot.loc[(thresh10_pivot[2015] != -9999), 'TOTAL 2001-2015'] = thresh10_pivot.sum(axis=1)
    thresh10_pivot.loc[(thresh10_pivot[2015] == -9999), 'TOTAL 2001-2015'] = -9999

    for thresh in [15, 20, 25, 30, 50, 75]:
        df_subset = df[df.thresh == thresh]

        df_pivot = df_subset.pivot(index='Country', columns='year', values='area')
        df_pivot.loc[(df_pivot[2015] != -9999), 'TOTAL 2001-2015'] = df_pivot.sum(axis=1)
        df_pivot.loc[(df_pivot[2015] == -9999), 'TOTAL 2001-2015'] = -9999

        thresh10_pivot = pd.concat([thresh10_pivot, df_pivot], axis=1, join_axes=[thresh10_pivot.index])

    thresh10_pivot.to_excel(writer, 'Loss (2001-2015) by Country')

    writer.save()


def add_lookup(data_df):

    lkp_df = pd.read_sql('SELECT iso, adm1, adm2, name0, name1, name2 FROM adm_lkp', conn)
    lkp_df = lkp_df.groupby(['iso', 'name0']).size().reset_index()

    print 'got lkp df'

    del lkp_df[0]

    data_df = pd.merge(data_df, lkp_df, on=['iso'], how='left')

    return data_df
    
if __name__ == '__main__':
    main()
