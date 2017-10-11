import argparse
import multiprocessing
from Queue import Queue
from threading import Thread

from utilities.layer import Layer
from utilities import geop, load_gadm28


def main():

    parser = argparse.ArgumentParser(description='Generate Hansen-tiled TSVs for Hadoop PIP')
    parser.add_argument('--source', '-s', help='the source dataset', required=True)
    parser.add_argument('--col_list', '-c', help='columns to include in the output TSV from the source', nargs='+')

    args = parser.parse_args()

    # create queue
    q = Queue()

    # create layer object
    l = Layer(args.source, args.col_list)

    # load gadm28 into postGIS if it doesn't exist already
    load_gadm28.load()

    # intersect with tile footprint and add tiles to queue
    l.build_tile_list(q)

    # start our threads
    mp_count = multiprocessing.cpu_count() - 1

    for i in range(mp_count):
        worker = Thread(target=geop.postgis_intersect, args=(q,))
        worker.setDaemon(True)
        worker.start()

    # process all jobs in the queue
    q.join()

    l.upload_to_s3()

    
if __name__ == '__main__':
    main()
