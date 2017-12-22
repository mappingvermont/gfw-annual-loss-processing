import os

import requests
import pandas as pd
import geopandas as gpd
from fuzzywuzzy import process

import util, postgis_util as pg_util


def join_to_api_df(df):

    polyname = df.polyname.unique()[0]
    valid_polynames = get_api_polynames()

    # use fuzzy matching to guess proper match
    matched_polyname, score = process.extractOne(polyname, valid_polynames)
    print '{} corrected to {}'.format(polyname, matched_polyname)

    df.polyname = matched_polyname

    iso_str = "', '".join(df.iso.unique())
    id_1_str = ', '.join(df.id_1.unique().astype(str))
    id_2_str = ', '.join(df.id_2.unique().astype(str))

    # need to do some kind of lookup here to go from
    # input polyname to polynames used here:
    # https://production-api.globalforestwatch.org/v1/query/499682b1-3174-493f-ba1a-368b4636708e?sql=SELECT%20polyname,%20count(*)%20FROM%20data%20GROUP%20BY%20polyname

    sql = ("SELECT * FROM data WHERE "
           "polyname = '{}' AND "
           "thresh = 30 AND "
           "iso in ('{}') AND adm1 in ({}) "
           "AND adm2 in ({}) ").format(
        matched_polyname, iso_str, id_1_str, id_2_str)

    print sql

    dataset_url = 'https://production-api.globalforestwatch.org/v1/query/499682b1-3174-493f-ba1a-368b4636708e'
    r = requests.get(dataset_url, params={'sql': sql})
    resp = r.json()['data'][0]

    base_data = resp.copy()
    del base_data['year_data']

    row_list = []

    for year_row in resp['year_data']:
        row = util.merge_two_dicts(base_data, year_row)
        row_list.append(row)

    api_df = pd.DataFrame(row_list)

    # match API column names, add thresh
    df = df.rename(columns={'id_1': 'adm1', 'id_2': 'adm2'})
    df['thresh'] = 30

    for field_name in ['bound1', 'bound2', 'bound3', 'bound4', 'year']:
        df[field_name] = df[field_name].replace('', -9999)
        df[field_name] = df[field_name].astype(int)

    field_list = ['polyname', 'bound1', 'bound2', 'bound3', 'bound4', 'iso', 'adm1', 'adm2', 'thresh', 'year']
    merged = pd.merge(df, api_df, how='left', on=field_list, suffixes=['_zstats', '_hadoop'])

    return merged


def compare_outputs(joined_df):

    for skip_col_name in ['_id', 'emissions', 'area_extent', 'area_gadm28']:
        del joined_df[skip_col_name]

    compare_cols = [x for x in joined_df.columns if 'hadoop' in x]

    for hadoop_col in compare_cols:
        zstats_col = hadoop_col.replace('hadoop','zstats')
        output_col = hadoop_col.replace('hadoop', 'pct_diff')

        joined_df[output_col] = abs(((joined_df[hadoop_col] - joined_df[zstats_col]) / joined_df[zstats_col]) * 100)

    print joined_df.head()
    print joined_df.shape


def get_api_polynames():
    dataset_url = 'http://production-api.globalforestwatch.org/v1/query/499682b1-3174-493f-ba1a-368b4636708e'
    params = {'sql': 'SELECT polyname FROM data GROUP BY polyname'}

    r = requests.get(dataset_url, params=params)

    return [x['polyname'] for x in r.json()['data']]


def filter_valid_adm2_boundaries(tile_name):

    tile_id = os.path.splitext(tile_name)[0].split('__')[-1]

    grid_df = gpd.read_file(os.path.join('grid', 'lossdata_footprint_filter.geojson'))
    grid_df = grid_df[grid_df.ID == tile_id].reset_index()
    bounds = grid_df.ix[0].geometry.bounds

    engine = pg_util.sqlalchemy_engine()

    sql = ('SELECT iso, id_1, id_2 '
           'FROM adm2_final '
           'WHERE ST_Contains(ST_MakeEnvelope( '
           '{}, {}, {}, {}, 4326), geom) '
           'GROUP BY iso, id_1, id_2 ').format(*bounds)

    adm2_df = pd.read_sql_query(sql, con=engine)

    return [tuple(x) for x in adm2_df.values]
