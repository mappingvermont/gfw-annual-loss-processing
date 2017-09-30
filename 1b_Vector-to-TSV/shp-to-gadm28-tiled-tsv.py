import argparse


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

    # interate over the tile list (multi thread)
    for t in l.tile_list():

        # download corresponding gadm28 tile + intersect
        t.intersect_gadm28()

    l.upload_to_s3()
