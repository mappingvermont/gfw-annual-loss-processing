import argparse
import os
import subprocess
import json
import requests

from rasterstats import zonal_stats
import pandas as pd
import geopandas as gpd
from fuzzywuzzy import process

from utilities import util, decode_tsv, layer, s3_list_tiles


def main():
    parser = argparse.ArgumentParser(description='QC loss and extent stats')
    parser.add_argument('--number-of-tiles', '-n', help='number of tiles to run QC', default=10, type=int, required=True)
    parser.add_argument('--grid-resolution', '-g', help='grid resolution of source', type=int, default=10, choices=(10, 0.25), required=True)

    parser.add_argument('--s3-poly-dir', '-s', help='input poly directory for intersected TSVs', required=True)
    parser.add_argument('--output-dataset-id', '-o', help='the output dataset in the API to compare results', required=True)

    parser.add_argument('--test', dest='test', action='store_true')

    args = parser.parse_args()
    util.start_logging()

    tile_list = s3_list_tiles.pull_random(args.s3_poly_dir, args.number_of_tiles)
    tile_list = ['ifl_2013__gfw_manged_forests__50N_090W.tsv']
    print tile_list

    temp_dir = util.create_temp_dir()

    # need to make a layer dir for this . . . abstract to util module
    qc_output_tile(tile_list[0], args.s3_poly_dir, temp_dir)


def qc_output_tile(tile_name, s3_src_dir, temp_dir):

    local_geojson = convert_to_geojson(tile_name, s3_src_dir, temp_dir)

    valid_admin_list = filter_valid_adm2_boundaries(tile_name)

    df = calc_zstats(local_geojson, valid_admin_list)

    joined = join_to_api_df(df)

    compare_outputs(joined)


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
    


def filter_valid_adm2_boundaries(tile_name):

    tile_id = os.path.splitext(tile_name)[0].split('__')[-1]

    grid_df = gpd.read_file(os.path.join('grid', 'lossdata_footprint_filter.geojson'))
    grid_df = grid_df[grid_df.ID == tile_id].reset_index()
    bounds = grid_df.ix[0].geometry.bounds

    engine = util.sqlalchemy_engine()

    sql = ('SELECT iso, id_1, id_2 '
           'FROM adm2_final '
           'WHERE ST_Contains(ST_MakeEnvelope( '
           '{}, {}, {}, {}, 4326), geom) '
           'GROUP BY iso, id_1, id_2 ').format(*bounds)

    adm2_df = pd.read_sql_query(sql, con=engine)

    return [tuple(x) for x in adm2_df.values]


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
        row = merge_two_dicts(base_data, year_row)
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


def convert_to_geojson(tile_name, s3_src_dir, temp_dir):

    # copy down chosen tile to the temp directory
    s3_path = '{}{}'.format(s3_src_dir, tile_name)
    cmd = ['aws', 's3', 'cp', s3_path, temp_dir]
    subprocess.check_call(cmd)

    # write the VRT
    local_tsv = os.path.join(temp_dir, tile_name)
    local_vrt = os.path.join(temp_dir, tile_name.replace('.tsv', '.vrt'))
    decode_tsv.build_vrt(local_tsv, local_vrt)

    # convert VRT to geojson
    local_geojson = os.path.join(temp_dir, tile_name.replace('.tsv', '.geojson'))

    cmd = ['ogr2ogr', '-f', 'GeoJSON', local_geojson, local_vrt]
    subprocess.check_call(cmd)

    # open in geopandas
    df = gpd.read_file(local_geojson)

    # dissolve by attributes to make queries to the API easier
    del df['field_1']
    dissolve_fields = list(df.columns)[0:-1]

    dissolved = df.dissolve(by=dissolve_fields).reset_index()

    # give columns their proper names
    dissolved.columns = ['polyname', 'bound1', 'bound2', 'bound3', 'bound4',
                         'iso', 'id_1', 'id_2', 'geometry']


    print dissolved.head()

    # gpd can't overwrite, need to delete file first
    os.remove(local_geojson)
    dissolved.to_file(local_geojson, driver='GeoJSON')

    return local_geojson



def calc_zstats(local_geojson, valid_adm2_tuples):

    resp_list = []

    with open(local_geojson) as thefile:
        data = json.load(thefile)

    url = 'https://0yvx7602sb.execute-api.us-east-1.amazonaws.com/dev/umd-loss-gain'

    for feat in data['features']:
        props = feat['properties']

        if (props['iso'], int(props['id_1']), int(props['id_2'])) in valid_adm2_tuples: 
            print feat['properties']

            payload = {'geojson': {'features': [feat]}}
            params = {'aggregate_values': False}

            r = requests.post(url, json=payload, params=params)
            resp = r.json()

            valid_zstats = False

            try:
                data = resp['data']['attributes']
                valid_zstats = True
            except KeyError:
                print resp

            if valid_zstats:
                print data

                for loss_year, loss_val in data['loss'].iteritems():
                    resp_dict = {'year': loss_year, 'area_loss': loss_val, 'area_gain': data['gain'],
                                 'area_extent_2000': data['treeExtent'], 'area_poly_aoi': data['areaHa']}

                    row = merge_two_dicts(feat['properties'], resp_dict)
                    resp_list.append(row)

    return pd.DataFrame(resp_list)


def merge_two_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None

    return z


def calc_zstats_rasterio(local_geojson):

    # run the rasterstats process, returing the geojson object with stats
    loss_vrt = r's3://gfw2-data/forest_change/hansen_2016_masked_30tcd/data.vrt'
    kwargs = {'categorical': True, 'geojson_out': True, 'prefix': '_'}
    geojson_with_stats = zonal_stats(local_geojson, loss_vrt, **kwargs)

    # convert to GDF
    df = gpd.GeoDataFrame.from_features(geojson_with_stats)

    # calculate approximate area of pixel for each poly, based on latitude
    # of the polygon's centroid
    df['pixel_area'] = df.apply(lambda row: util.get_pixel_area(row.geometry.centroid.y), axis=1)

    # normalize the df and create a years field
    # make a unique column list, filtering out all the _ prefixed columns
    unique_cols = list([x for x in df.columns if x[0] != '_' and x != 'geometry'])

    df = pd.wide_to_long(df, '_', unique_cols, 'year').reset_index()

    df.year = df.year.astype(int) + 2000
    df = df.rename(columns={'_': 'pixel_count', 'field_7': 'iso', 'field_8': 'id_1', 'field_9': 'id_2'})

    df.id_1 = df.id_1.astype(int)
    df.id_2 = df.id_2.astype(int)

    df['loss_ha'] = df.pixel_area * df.pixel_count

    return df


def get_api_polynames():

    dataset_url = 'http://production-api.globalforestwatch.org/v1/query/499682b1-3174-493f-ba1a-368b4636708e'
    params = {'sql': 'SELECT polyname FROM data GROUP BY polyname'}

    r = requests.get(dataset_url, params=params)
    
    return [x['polyname'] for x in r.json()['data']]


if __name__ == '__main__':
    main()


