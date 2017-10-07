import argparse
import multiprocessing

from utilities.layer import Layer
from utilities import util


def main():

    parser = argparse.ArgumentParser(description='Generate Hansen-tiled TSVs for Hadoop PIP')
    parser.add_argument('--source', '-s', help='the source dataset', required=True)

    args = parser.parse_args()

    # create data folder and download Hansen tile geojson
    util.download_hansen_footprint()

    # create layer object
    l = Layer(args.source)

    # intersect with Hansen to figure out what tiles we have
    l.build_tile_list()
    
    # multithread the clipping
    mp_count = multiprocessing.cpu_count() - 1
    pool = multiprocessing.Pool(processes=mp_count)

    pool.map(util.postgis_intersect, l.tile_list)
        
    #l.upload_to_s3()

    
if __name__ == '__main__':
    main()
