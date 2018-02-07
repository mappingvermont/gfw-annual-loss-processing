import os
import uuid
import multiprocessing
import subprocess
from threading import Thread
from Queue import Queue
import logging
import math

import fiona
import numpy as np
import geopandas as gpd
import rasterio
from shapely.geometry import shape

from tile import Tile
import decode_tsv
import postgis_util as pg_util


def create_temp_dir():

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(root_dir, 'data')
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    guid = str(uuid.uuid4())
    layer_dir = os.path.join(data_dir, guid)

    os.mkdir(layer_dir)

    return layer_dir


def exec_multiprocess(input_func, input_list, is_test=False, thread_count=False):

    # start our threads
    if is_test:
        mp_count = 1
    elif thread_count:
        mp_count = thread_count
    else:
        mp_count = multiprocessing.cpu_count() - 1

    # create queue
    q = Queue()

    for i in input_list:
        q.put(i)

    for i in range(mp_count):
        worker = Thread(target=input_func, args=(q,))
        worker.setDaemon(True)
        worker.start()

    # process all jobs in the queue
    q.join()


def build_gadm28_tile_list(source_layer, is_test):

    logging.info('Building tile list')
    logging.info('checking extent of input geometry {}'.format(source_layer.input_dataset))

    # shapefile of tiles used to tsv aoi
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tiles = fiona.open(os.path.join(root_dir, 'grid', 'lossdata_footprint_filter.geojson'), 'r')

    # aoi we want to tsv
    source_file_ext = os.path.splitext(source_layer.input_dataset)[1]
    if source_file_ext in ['.rvrt', '.tif']:
        with rasterio.open(source_layer.input_dataset) as src:
            aoi_bounds = src.bounds
    elif source_file_ext == '':
        conn, cursor = pg_util.conn_to_postgis()

        extent_sql = 'SELECT ST_Extent(geom) FROM {}'.format(source_layer.input_dataset)
        cursor.execute(extent_sql)
        extent_text = cursor.fetchall()[0][0].replace('BOX(', '').replace(')','').replace(',', ' ')
        aoi_bounds = [float(x) for x in extent_text.split()] 

    else:
        aoi_bounds = fiona.open(source_layer.input_dataset).bounds

    # select tiles that are inside of the bounding box of the aoi
    tiles_in_aoi = tiles.filter(bbox=aoi_bounds)

    for feat in tiles_in_aoi:
        # get the bounding box of the 1deg tile
        bbox = shape(feat['geometry']).bounds

        # get the tile id- used for naming
        # tile_id = feat['properties']['ulx_uly']
        tile_id = feat['properties']['ID']

        # build the tile object
        t = Tile(source_layer.input_dataset, source_layer.col_list, tile_id, bbox)

        # add the tile bbox to the tile_list
        source_layer.tile_list.append(t)

    # if this is a test, only do one tile
    if is_test:
        source_layer.tile_list = source_layer.tile_list[0:1]


def start_logging():

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_file = os.path.join(root_dir, 'output.log')
    logging.basicConfig(filename=log_file, level=logging.INFO)

    logging.getLogger('botocore').setLevel(logging.ERROR)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)


def get_pixel_area(lat):
    """
    Calculate geodesic area for Hansen data, assuming a fix pixel size of 0.00025 * 0.00025 degree
    using WGS 1984 as spatial reference.
    Pixel size various with latitude, which is it the only input parameter
    """
    a = 6378137.0  # Semi major axis of WGS 1984 ellipsoid
    b = 6356752.314245179  # Semi minor axis of WGS 1984 ellipsoid

    d_lat = 0.00025  # pixel hight
    d_lon = 0.00025  # pixel width

    pi = math.pi

    q = d_lon / 360
    e = math.sqrt(1 - (b / a) ** 2)

    area = abs(
        (pi * b ** 2 * (
            2 * np.arctanh(e * np.sin(np.radians(lat + d_lat))) /
            (2 * e) +
            np.sin(np.radians(lat + d_lat)) /
            ((1 + e * np.sin(np.radians(lat + d_lat))) * (1 - e * np.sin(np.radians(lat + d_lat)))))) -
        (pi * b ** 2 * (
            2 * np.arctanh(e * np.sin(np.radians(lat))) / (2 * e) +
            np.sin(np.radians(lat)) / ((1 + e * np.sin(np.radians(lat))) * (1 - e * np.sin(np.radians(lat))))))) * q

    return area


def merge_two_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None

    return z


def s3_to_gdf(s3_src_dir, tile_name, output_dir):

    logging.info('running s3 --> geojson')

    # copy down chosen tile to the temp directory
    s3_path = '{}{}'.format(s3_src_dir, tile_name)
    cmd = ['aws', 's3', 'cp', s3_path, output_dir]
    subprocess.check_call(cmd)

    # write the VRT
    local_tsv = os.path.join(output_dir, tile_name)
    local_vrt = os.path.join(output_dir, tile_name.replace('.tsv', '.vrt'))
    decode_tsv.build_vrt(local_tsv, local_vrt)

    # convert VRT to geojson
    local_geojson = os.path.join(output_dir, tile_name.replace('.tsv', '.geojson'))

    cmd = ['ogr2ogr', '-f', 'GeoJSON', local_geojson, local_vrt]
    subprocess.check_call(cmd)

    # open in geopandas
    logging.info('reading {} with gpd'.format(local_geojson))
    try:
        df = gpd.read_file(local_geojson)
        logging.info('read {} into gpd complete'.format(local_geojson))

    # likely a geometrycollection in the geojson
    # requires further manual investigation
    except ValueError:
        logging.error('could not read {} in {} using geopandas'.format(tile_name, output_dir))
        df = gpd.GeoDataFrame()

    return df, local_geojson


def dissolve_tsv(df, local_geojson):

    # remove garbage field_1
    # already read in as geometry field by geopandas
    del df['field_1']

    # dissolve by attributes to reduce number of queries to the API
    dissolve_fields = list(df.columns)[0:-1]
    valid_dissolve = False

    logging.info('starting dissolve for {}'.format(local_geojson))

    try:
        dissolved = df.dissolve(by=dissolve_fields).reset_index()
        logging.info('dissolve for {} complete'.format(local_geojson))
        valid_dissolve = True

    # occasionally fails with bad geometry
    except ValueError:
        logging.info('dissolve failed for input {}'.format(local_geojson))

    if valid_dissolve:

        # give columns their proper names
        dissolved.columns = ['polyname', 'bound1', 'bound2', 'bound3', 'bound4',
                             'iso', 'id_1', 'id_2', 'geometry']

        # gpd can't overwrite, need to delete file first
        dissolved_path = os.path.splitext(local_geojson)[0] + '_dissolved.geojson'

        try:
            dissolved.to_file(dissolved_path, driver='GeoJSON')
        except ValueError:
            valid_dissolve = False

    if valid_dissolve:
        return dissolved_path
    else:
        return False


def subset_geojson(local_geojson, record_count):

    # potential instances where the input is False
    # where a previous dissolve has failed, and we don't want to continue
    if local_geojson:

	    gdf = gpd.read_file(local_geojson)
	    
	    if len(gdf) > record_count:
		gdf = gdf.sample(record_count)

	    os.remove(local_geojson)
	    gdf.to_file(local_geojson, driver='GeoJSON')

    return local_geojson

