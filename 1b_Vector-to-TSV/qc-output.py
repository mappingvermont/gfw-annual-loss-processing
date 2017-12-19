import argparse
import os
import subprocess

from rasterstats import zonal_stats
import pandas as pd
import geopandas as gpd

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

    df = calc_zstats(local_geojson)

    df = filter_valid_adm2_boundaries(df, tile_name)

    compare_to_api(df)

    print df.head()


def filter_valid_adm2_boundaries(zstats_df, tile_name):
    
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

    return pd.merge(zstats_df, adm2_df, on=['iso', 'id_1', 'id_2'])


def compare_to_api(df):
    
    polyname = df.field_2.unique()[0]
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
            polyname, iso_str, id_1_str, id_2_str)

    print sql
    # make requests
    # parse json
    # load into DF + unpack nested year values
    # compare to df
    # write to database or s3?
    


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

    return local_geojson


def calc_zstats(local_geojson):

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
    

if __name__ == '__main__':
    main()
