import argparse
import multiprocessing

from utilities.layer import Layer
from utilities import util


def main():

    parser = argparse.ArgumentParser(description='Generate Hansen-tiled TSVs for Hadoop PIP')
    parser.add_argument('--source', '-s', help='the source dataset', required=True)
    parser.add_argument('--col_list', '-c', help='columns to include in the output TSV from the source', nargs='+')

    args = parser.parse_args()

    # create layer object
    l = Layer(args.source, args.col_list)

    # intersect with Hansen to figure out what tiles we have
    l.build_tile_list()
    
    # multithread the clipping
    mp_count = multiprocessing.cpu_count() - 1
    pool = multiprocessing.Pool(processes=1)

    pool.map(util.postgis_intersect, l.tile_list)
        
    #l.upload_to_s3()

    
if __name__ == '__main__':
    main()
