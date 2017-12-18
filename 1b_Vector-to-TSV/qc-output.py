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
    print tile_list

    temp_dir = util.create_temp_dir()

    # need to make a layer dir for this . . . abstract to util module
    qc_output_tile(tile_list[0], args.s3_poly_dir, temp_dir)
    


def qc_output_tile(tile_name, s3_src_dir, temp_dir):
    
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

    # run the rasterstats process, returing the geojson object with stats 
    loss_vrt = r's3://gfw2-data/forest_change/hansen_2016_masked_30tcd/data.vrt'
    kwargs = {'categorical': True, 'geojson_out': True, 'prefix': '_'}
    geojson_with_stats = zonal_stats(local_geojson, loss_vrt, **kwargs) 
    
    # convert to GDF
    df = gpd.GeoDataFrame.from_features(geojson_with_stats)
    print df.head()

    # calculate approximate area of pixel for each poly, based on latitude
    # of the polygon's centroid
    df['pixel_area'] = df.apply(lambda row: util.get_pixel_area(row.geometry.centroid.y), axis=1)
    print df.head()

    # normalize the df and create a years field
    df = pd.wide_to_long(df, '_', ['field_7', 'field_8', 'field_9'], 'year')
    df.year = df.year.astype(int) + 2000
    df = df.reset_index()
    print df.head()    
    

if __name__ == '__main__':
    main()
