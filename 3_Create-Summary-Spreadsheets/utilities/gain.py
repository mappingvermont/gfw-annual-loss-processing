import pandas as pd

import util


def build_df(adm_level, iso, extent_year):

    print 'starting gain for adm level {}'.format(adm_level)

    field_list = util.level_lkp(adm_level)
    field_text = ', '.join(field_list)

    sql = 'SELECT {}, sum(area) as area FROM gain '.format(field_text)

    if iso:
        sql += "WHERE iso = '{}' ".format(iso)

    sql += 'GROUP BY {}'.format(field_text)

    conn = util.db_connect()
    df = pd.read_sql(sql, conn)
    df = util.add_lookup(df, adm_level, conn)
    
    # dealing with raw data, so need to convert it
    # all change column name for output spreadsheet
    df['area'] = df.area / 10000

    # Create expression to come up with a combined field name
    # if iso, just Country, if adm1, Country_Adm1_Name, etc
    df['Country_Index'] = eval(util.country_text_lookup(adm_level))
    
    # group and sum just in case, and to remove additional columns
    df = df.groupby(['Country_Index'])['area'].sum().reset_index()

    # Add larger index for merged column in output excel sheet
    df['All areas are in hectares'] = 'TREE COVER GAIN'
    df['Country'] = '(>50% CANOPY COVER)'

    # Pivot so that these columns (which all have the same values) go to the top of the DF as indices
    df = df.pivot_table(columns=['All areas are in hectares', 'Country'], index='Country_Index')

    # remove extraneous area level
    df.columns = df.columns.droplevel(0)

    # remove the index name level as well
    del df.index.name
    
    sheet_name_dict = {0: 'Country', 1: 'Subnat1', 2: 'Subnat2'}
    sheet_type = sheet_name_dict[adm_level]

    sheet_name = 'Gain (2001-2012) by {}'.format(sheet_type)

    return sheet_name, df


