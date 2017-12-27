import os
import logging

import requests
import psycopg2
import pandas as pd
import geopandas as gpd
from fuzzywuzzy import process

import util, postgis_util as pg_util


def join_to_api_df(df):

    # replace common polyname suffix
    df.polyname = df['polyname'].apply(lambda x: x.replace('_int_diss_gadm28_large.tsv', '')) 

    tsv_polyname = df.polyname.unique()[0]
    valid_polynames = get_api_polynames()

    # reverse any ifl or plantations intersections
    # so that ifl/plantation polyname is in front
    if '__' in tsv_polyname and tsv_polyname[0:4] not in ('ifl_', 'plan'):
        split_poly = tsv_polyname.split('__')
        tsv_polyname = '__'.join([split_poly[1], split_poly[0]])

    # use fuzzy matching to guess proper match
    matched_polyname, score = process.extractOne(tsv_polyname, valid_polynames)
    logging.info('{} corrected to {}'.format(tsv_polyname, matched_polyname))

    # update polyname field for joining, save original polyname
    df.polyname = matched_polyname
    df['tsv_polyname'] = tsv_polyname

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

    logging.info(sql)

    dataset_url = 'https://production-api.globalforestwatch.org/v1/query/499682b1-3174-493f-ba1a-368b4636708e'
    r = requests.get(dataset_url, params={'sql': sql})
    resp = r.json()['data']

    row_list = []

    for grouped_row in resp:
        
        base_data = grouped_row.copy()
        del base_data['year_data']

        for year_dict in grouped_row['year_data']:
            print year_dict
            row = util.merge_two_dicts(base_data, year_dict)
            row_list.append(row)

    api_df = pd.DataFrame(row_list)

    # match API column names, add thresh
    df = df.rename(columns={'id_1': 'adm1', 'id_2': 'adm2'})
    df['thresh'] = 30

    for field_name in ['year', 'adm1', 'adm2']:
        df[field_name] = df[field_name].replace('', -9999)
        df[field_name] = df[field_name].astype(int)

    for field_name in ['bound1', 'bound2', 'bound3', 'bound4']:
        df[field_name] = df[field_name].replace('', '-9999')
        df[field_name] = df[field_name].astype(str)

    api_df.to_csv('api_df.csv', index=None)
    df.to_csv('df.csv', index=None)

    print api_df.bound1.dtype
    print df.bound1.dtype

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

    engine = pg_util.sqlalchemy_engine()

    # save results to postgres
    joined_df.to_sql('qc_results', engine, if_exists='append')


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

    # standard ST_Contains query
    # but provinces with -9999 because the API throws an error
    sql = ('SELECT iso, id_1, id_2 '
           'FROM adm2_final '
           'WHERE id_1 != -9999 AND id_2 != -9999 AND ' 
           'ST_Contains(ST_MakeEnvelope( '
           '{}, {}, {}, {}, 4326), geom) '
           'GROUP BY iso, id_1, id_2 ').format(*bounds)

    adm2_df = pd.read_sql_query(sql, con=engine)

    return [tuple(x) for x in adm2_df.values]


def check_results():

    creds = pg_util.get_creds()
    conn = psycopg2.connect(**creds)
    
    cursor = conn.cursor()
    cursor.execute('SELECT max(area_extent_2000_pct_diff), max(area_gain_pct_diff), max(area_loss_pct_diff), max(area_poly_aoi_pct_diff) FROM qc_results')

    pct_diff_list = cursor.fetchall()[0]
    print 'Max pct diff values from qc_results table:'
    print pct_diff_list
    
    conn.close()

    if max(pct_diff_list) > 1:
        raise ValueError('Max percent error >= 1%')

