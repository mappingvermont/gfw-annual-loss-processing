import os
import subprocess
import multiprocessing
from threading import Thread
from Queue import Queue
import fiona
from shapely.geometry import shape

import geop
from layer import Layer
from tile import Tile


def get_creds():

    try:
        creds = {'host': os.environ['PG_HOST'],
                 'user': os.environ['PG_USER'],
                 'password': os.environ['PG_PASS'],
                 'dbname': os.environ['PG_DBNAME']}

    except KeyError:
        creds = {'host': 'localhost', 'password': 'gis', 'dbname': 'gis', 'user': 'gis'}

    return creds


def find_tile_overlap(layer_a, layer_b):

    print 'finding tile overlap'

    # looks at the s3 directory to figure out what tiles both "layers" have in common based on tile id strings


def export(table_name, tile, creds):

    shp_dir = os.path.join(tile.out_dir, 'shp')

    if not os.path.exists(shp_dir):
        os.mkdir(shp_dir)

    shp_filename = tile.tile_id + '.shp'

    to_shp_cmd = ['pgsql2shp', '-u', creds['user'], '-P', creds['password'], '-h', 'localhost',
                  creds['dbname'], table_name.lower(), '-f', shp_filename]
    print to_shp_cmd

    # for some reason can't specify a full output path, just a filename
    # to choose dir, set it to CWD
    subprocess.check_call(to_shp_cmd, cwd=shp_dir)

    out_shp = os.path.join(shp_dir, table_name.lower() + '.shp')
    out_geojson = os.path.join(tile.out_dir, tile.tile_id + '.geojson')

    # this will ultimately be a TSV, using geojson for now to QC
    to_geojson_cmd = ['ogr2ogr', '-f', 'GeoJSON', out_geojson, out_shp]
    print to_geojson_cmd

    subprocess.check_call(to_geojson_cmd)


def boundary_field_to_sql(field_name):

    if 'boundary_field' in field_name:
        field_name = '1 AS ' + field_name

    return field_name


def table_has_rows(cursor, table_name):
    has_rows = False

    # source: https://stackoverflow.com/questions/4138734/
    cursor.execute('SELECT count(*) FROM (SELECT 1 FROM {} LIMIT 1) AS t'.format(table_name))

    if cursor.fetchone()[0]:
        has_rows = True

    return has_rows


def exec_multiprocess(input_func, input_list):

    # create queue
    q = Queue()

    for i in input_list:
        q.put(i)

    # start our threads
    # mp_count = multiprocessing.cpu_count() - 1
    mp_count = 1

    for i in range(mp_count):
        worker = Thread(target=input_func, args=(q,))
        worker.setDaemon(True)
        worker.start()

    # process all jobs in the queue
    q.join()


def intersect_gadm(source_layer, gadm_layer):

    input_list = []

    output_layer = Layer(None, [])

    for t in source_layer.tile_list:
        input_list.append((output_layer, t, gadm_layer.tile_list[0]))

    exec_multiprocess(geop.intersect_layers, input_list)

    return output_layer


def build_gadm28_tile_list(source_layer, is_test):

    print 'Building tile list'
    print 'checking extent of input geometry {}'.format(source_layer.input_dataset)

    # shapefile of tiles used to tsv aoi
    # tiles = fiona.open(os.path.join('grid', 'footprint_1degree.shp'), 'r')
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tiles = fiona.open(os.path.join(root_dir, 'grid', 'lossdata_footprint_filter.geojson'), 'r')

    # aoi we want to tsv (take this out)
    aoi = fiona.open(source_layer.input_dataset)

    # select tiles that are inside of the bounding box of the aoi
    tiles_in_aoi = tiles.filter(bbox=aoi.bounds)

    for feat in tiles_in_aoi:
        # get the bounding box of the 1deg tile
        bbox = shape(feat['geometry']).bounds

        # get the tile id- used for naming
        # tile_id = feat['properties']['ulx_uly']
        tile_id = feat['properties']['ID']

        # build the tile object
        t = Tile(source_layer.input_dataset, source_layer.col_list, tile_id, bbox, source_layer.layer_dir)

        # add the tile bbox to the tile_list
        source_layer.tile_list.append(t)

    # if this is a test, only do one tile
    if is_test:
        source_layer.tile_list = source_layer.tile_list[0:1]
