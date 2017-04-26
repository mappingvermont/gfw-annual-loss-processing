import pandas as pd

import util


def build_df(adm_level, iso=None):

    print 'starting extent for adm level {}'.format(adm_level)

    field_list = util.level_lkp(adm_level)
    field_text = ', '.join(field_list)

    sql = 'SELECT {}, thresh, sum(area) as area FROM extent '.format(field_text)

    if iso:
        sql += "WHERE iso = '{}' ".format(iso)

    sql += 'GROUP BY {}, thresh'.format(field_text)

    conn = util.db_connect()
    df = pd.read_sql(sql, conn)
    df = util.add_lookup(df, adm_level, conn)

    # Create expression to come up with a combined field name
    # if iso, just Country, if adm1, Country_Adm1_Name, etc
    df['Country'] = eval(util.country_text_lookup(adm_level))

    # Group by Country and thresh, sum area
    df = df.groupby(['Country', 'thresh'])['area'].sum().reset_index()

    # pivot and remove column where thresh == 0
    df_pivot = df.pivot(index='Country', columns='thresh', values='area').reset_index()
    del df_pivot[0]

    # rename columns to match output
    df_pivot.columns = ['Country', '>10%', '>15%', '>20%', '>25%', '>30%', '>50%', '>75%']

    sheet_name_dict = {0: 'Country', 1: 'Subnat1', 2: 'Subnat2'}
    sheet_name = 'Extent (2000) by {}'.format(sheet_name_dict[adm_level])

    return sheet_name, df_pivot


