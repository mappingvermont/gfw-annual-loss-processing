import argparse
import logging

from utilities.layer import Layer
from utilities import util, decode_tsv


def main():
    parser = argparse.ArgumentParser(description='Tile an existing TSV')
    parser.add_argument('--input-s3-path', '-i', help='the input dataset as vector VRT', required=True)

    parser.add_argument('--output-format', '-o', help='output format', default='tsv', choices=('tsv', 'shp', 'geojson'))
    parser.add_argument('--output-name', '-n', help='output name', required=True)
    parser.add_argument('--s3-out-dir', '-s', help='s3 out dir', default='s3://gfw2-data/alerts-tsv/tsv-boundaries-tiled/')

    parser.add_argument('--test', dest='test', action='store_true')

    args = parser.parse_args()
    logging.basicConfig(filename='output.log', level=logging.DEBUG)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

    # create layer object
    l = Layer(args.input_s3_path, None)

    # download dataset from s3 and create VRT
    decode_tsv.decode(l)

    # create blueprint for the source dataset based on Hansen grid
    util.build_gadm28_tile_list(l, args.test)

    # export the source TSV to a clipped, tiled TSV
    # multithreaded given that this is the only process
    # other exports are already multithreaded because they're attached
    # to a multithread clip/intersection operation
    l.export(args.output_name, args.output_format)

    l.upload_to_s3(args.output_format, args.s3_out_dir, args.test)


if __name__ == '__main__':
    main()
