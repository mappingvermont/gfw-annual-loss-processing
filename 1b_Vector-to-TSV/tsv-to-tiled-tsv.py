import argparse

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
    util.start_logging()

    # create layer object
    l = Layer(args.input_s3_path, None)

    # download dataset from s3 and create VRT
    decode_tsv.decode(l)

    # create blueprint for the source dataset based on Hansen grid
    util.build_gadm28_tile_list(l, args.test)

    # export the source TSV to a clipped, tiled TSV
    l.export(args.output_name, args.output_format)

    l.upload_to_s3(args.output_format, args.s3_out_dir, args.test)


if __name__ == '__main__':
    main()
