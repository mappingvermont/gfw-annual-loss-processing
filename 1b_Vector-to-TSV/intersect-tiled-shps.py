import argparse


from utilities.layer import Layer
from utilities import util


def main():

    parser = argparse.ArgumentParser(description='Intersect two pre-tiled datasets to create union tiles')
    parser.add_argument('--dataset-a', '-a', help='s3 path (with wildcard) to dataset A', required=True)
    parser.add_argument('--dataset-b', '-b', help='s3 path (with wildcard) to dataset B', required=True)
    parser.add_argument('--output-format', '-o', help='output format', default='tsv', choices=('tsv', 'shp', 'geojson'))
    parser.add_argument('--output-name', '-n', help='output name', required=True)
    parser.add_argument('--s3-out-dir', '-s', help='s3 out dir', default='s3://gfw2-data/alerts-tsv/gadm28-poly-tsvs/')

    args = parser.parse_args()

    layer_a = Layer(args.dataset_a, None, None, args.output_format, args.s3_out_dir)
    layer_b = Layer(args.dataset_b, None, None, args.output_format, args.s3_out_dir)

    # figure out what tiles they have in common
    # this is done just by looking at the directory on s3, not locally
    overlap_list = util.find_tile_overlap(layer_a, layer_b)

    # download all tiles in common
    # this builds layer.tile_list in each separate layer
    for overlap_tile in overlap_list:
        layer_a.download_s3_tile(overlap_tile)
        layer_b.download_s3_tile(overlap_tile)

    # need to multithread this
    layer_c = util.intersect_layers(layer_a, layer_b)

    layer_c.upload_to_s3()






