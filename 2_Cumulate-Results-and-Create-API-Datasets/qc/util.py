import subprocess
import pandas as pd
from fuzzywuzzy import process


def get_dfs():

    # get csv of gadm shapefile
    sql = "SELECT GID_0, NAME_0 FROM gadm_3_6_adm2 GROUP BY GID_0, NAME_0"
    cmd = ['ogr2ogr', '-f', 'CSV', '-dialect', 'sqlite', '-sql', sql, 'gadm36.csv', 'gadm/gadm_3_6_adm2.shp']

    subprocess.check_call(cmd)

    # read in peter's names to dataframe
    peters = pd.read_excel('Stat_2017.xlsx')
    # drop the first row which is 'outside any'
    peters = peters.drop([0])

    gadm36 = pd.read_csv('gadm36.csv')

    return peters, gadm36


def create_peters_df():
    peters, gadm36 = get_dfs()

    # get names of ISO countries
    valid_polynames = gadm36['NAME_0'].values.tolist()

    # match peter's country to ISO country
    peters['matched_polyname'] = peters['Name'].apply(lambda x: process.extractOne(x, valid_polynames)[0])

    # print where match is < 100
    peters['score'] = peters['Name'].apply(lambda x: process.extractOne(x, valid_polynames)[1])
    print "NAMES WITH MATCH SCORE LESS THAN 100%\n"
    print peters[peters['score'] < 100][['Name', 'matched_polyname']]

    # fix a few that didn't match well
    peters.loc[peters['Name'] == 'US Virgin Islands', 'matched_polyname'] = 'Virgin Islands, U.S.'

    # GET THE ISO! join Peters dataframe 'matched_polyname' to the gadm36 data 'NAME_0'
    peters = peters.merge(gadm36, how='left', left_on='matched_polyname', right_on='NAME_0')

    # turn table into pivot table
    peters_headers = list(peters.columns.values)
    loss_yrs = [x for x in peters_headers if "Loss_" in x]

    peters = pd.melt(peters, id_vars=['GID_0'], value_vars=loss_yrs)

    # create the column values to be 2001 instead of Loss_2001
    peters['year'] = peters['variable'].apply(lambda x: (x.split('_')[1]))
    peters['year'] = peters['year'].astype('int') - 2000

    # rename columns
    peters = peters.rename(columns={'GID_0': 'iso', 'value': 'area'})
    return peters


def create_loss_df():
    df = pd.read_csv('loss.csv', header=None, names=['polyname', 'bound1', 'bound2', 'bound3', 'bound4', 'iso',
                                                     'adm1', 'adm2', 'thresh', 'year', 'area', 'biomass'])


    # group and sum
    grouped = df.groupby(['iso', 'year'])[['area']].sum().reset_index()
    grouped['area'] = grouped['area'] / 10000
    return grouped


def calc_area_diff(peters_df, loss_df):
    # join hadoop df to peters_df
    joined = peters_df.merge(loss_df, on=['iso', 'year'], suffixes=['_peters', '_hadoop'])
    joined['perc_diff'] = ((joined['area_peters']-joined['area_hadoop'])/joined['area_peters']) * 100
    print joined.head()

    joined.to_csv('joined.csv')

