import pandas as pd

import util


def build_df(adm_level, iso=None):

    print 'starting loss for adm level {}'.format(adm_level)

    conn = util.db_connect()

    field_list = util.level_lkp(adm_level)
    field_text = ', '.join(field_list)

    sql = 'SELECT {}, thresh, year, sum(area) as area FROM loss '.format(field_text)

    if iso:
        sql += "WHERE iso = '{}' ".format(iso)

    sql += 'GROUP BY {}, thresh, year'.format(field_text)

    df = pd.read_sql(sql, conn)
    df = util.add_lookup(df, adm_level, conn)

    # Create expression to come up with a combined field name
    # if iso, just Country, if adm1, Country_Adm1_Name, etc
    df['Country'] = eval(util.country_text_lookup(adm_level))

    is_first = True

    for thresh in [10, 15, 20, 25, 30, 50, 75]:
        df_subset = df[df.thresh == thresh]

        df_pivot = df_subset.pivot(index='Country', columns='year', values='area').reset_index()
        df_pivot['TOTAL 2001-2015'] = df_pivot.sum(axis=1)

        if is_first:
            output_df = df_pivot.copy()
            is_first = False

        else:
            del df_pivot['Country']
            output_df = pd.concat([output_df, df_pivot], axis=1, join_axes=[output_df.index])

    sheet_name_dict = {0: 'Country', 1: 'Subnat1', 2: 'Subnat2'}
    sheet_name = 'Loss (2001-2015) by {}'.format(sheet_name_dict[adm_level])

    return sheet_name, output_df


