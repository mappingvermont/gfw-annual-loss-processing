import argparse
import logging

from utilities.layer import Layer
from utilities import util, geop, load_gadm28


def main():
    parser = argparse.ArgumentParser(description='Generate Hansen-tiled TSVs for Hadoop PIP')
    parser.add_argument('--input-dataset', '-i', help='the input dataset', required=True)
    parser.add_argument('--col-list', '-c', help='columns to include in the output TSV from the source', nargs='+')

    parser.add_argument('--output-format', '-o', help='output format', default='tsv', choices=('tsv', 'shp', 'geojson'))
    parser.add_argument('--output-name', '-n', help='output name', required=True)
    parser.add_argument('--s3-out-dir', '-s', help='s3 out dir', default='s3://gfw2-data/alerts-tsv/gadm28-poly-tsvs/')

    parser.add_argument('--test', dest='test', action='store_true')

    args = parser.parse_args()
    logging.basicConfig(filename='output.log', level=logging.DEBUG)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

    # create layer object
    source_layer = Layer(args.input_dataset, args.col_list)

    # load gadm28 into postGIS if it doesn't exist already
    gadm28_layer = load_gadm28.load()

    # create blueprint for the source dataset in postgis
    util.build_gadm28_tile_list(source_layer, args.test)

    # process clip jobs above-- this loads tiles into PostGIS
    util.exec_multiprocess(geop.clip, source_layer.tile_list)

    # intersect all tiles of source layer with gadm28
    l = util.intersect_gadm(source_layer, gadm28_layer)

    # todo -- export this layer
    print l.tile_list[0].postgis_table

    l.upload_to_s3(args.output_name, args.output_format, args.s3_out_dir)


if __name__ == '__main__':
    main()
