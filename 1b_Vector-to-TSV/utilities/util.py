import os
import uuid
import multiprocessing
from threading import Thread
from Queue import Queue
import logging
import math
import numpy as np

import fiona
import rasterio
from shapely.geometry import shape

from tile import Tile


def create_temp_dir():

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(root_dir, 'data')
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    guid = str(uuid.uuid4())
    layer_dir = os.path.join(data_dir, guid)

    os.mkdir(layer_dir)

    return layer_dir


def exec_multiprocess(input_func, input_list, is_test=False):

    # create queue
    q = Queue()

    for i in input_list:
        q.put(i)

    # start our threads
    if is_test:
        mp_count = 1
    else:
        mp_count = multiprocessing.cpu_count() - 1

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
    if os.path.splitext(source_layer.input_dataset)[1] == '.tif':
        with rasterio.open(source_layer.input_dataset) as src:
            aoi_bounds = src.bounds
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
