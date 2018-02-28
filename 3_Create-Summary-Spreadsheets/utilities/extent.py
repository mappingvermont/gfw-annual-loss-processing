import pandas as pd

import util


def build_df(adm_level, iso, extent_year):

    print 'starting extent{} for adm level {}'.format(extent_year, adm_level)

    field_list = util.level_lkp(adm_level)
    field_text = ', '.join(field_list)

    sql = 'SELECT {}, thresh, sum(area) as area FROM extent{} '.format(field_text, extent_year)

    if iso:
        sql += "WHERE iso = '{}' ".format(iso)

    sql += 'GROUP BY {}, thresh'.format(field_text)

    conn = util.db_connect()
    df = pd.read_sql(sql, conn)
    df = util.add_lookup(df, adm_level, conn)
    
    # remove thresh 0 values
    df = df[df.thresh != 0]

    # Create expression to come up with a combined field name
    # if iso, just Country, if adm1, Country_Adm1_Name, etc
    df['Country_Index'] = eval(util.country_text_lookup(adm_level))

    # Group by Country and thresh, sum area
    df = df.groupby(['Country_Index', 'thresh'])['area'].sum().reset_index()

    # Add larger index for merged column in output excel sheet
    df['All areas are in hectares'] = 'TREE COVER ({}) BY PERCENT CANOPY COVER'.format(extent_year)
    
    # convert int thresh to labeled thresh percent
    df['thresh'] = df.apply(lambda row: '>{}%'.format(str(row['thresh'])), axis=1)
    
    # rename the 'thresh' column to the name that we'll need for our summary output table
    column_name_dict = {0: 'Country', 1: 'Country_Subnat1', 2: 'Country_Subnat1_Subnat2'}
    summary_col_name = column_name_dict[adm_level]
    
    df.rename(columns = {'thresh': summary_col_name}, inplace=True)
    
    # pivot and remove column where thresh == 0
    df_pivot = df.pivot_table(index=['Country_Index', 'All areas are in hectares'], columns=summary_col_name, values='area')
    
    df_pivot = df_pivot.unstack('All areas are in hectares')
    df_pivot = df_pivot.swaplevel(0,1, axis=1)
    del df_pivot.index.name

    sheet_name_dict = {0: 'Country', 1: 'Subnat1', 2: 'Subnat2'}
    sheet_name = 'Extent ({}) by {}'.format(extent_year, sheet_name_dict[adm_level])

    return sheet_name, df_pivot


