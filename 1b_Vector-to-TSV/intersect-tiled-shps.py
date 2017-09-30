import argparse


from utilities.layer import Layer
from utilities import util


def main():

    parser = argparse.ArgumentParser(description='Intersect two pre-tiled datasets to create union tiles')
    parser.add_argument('--dataset-a', '-a', help='ID on s3 for dataset A', required=True)
    parser.add_argument('--dataset-b', '-b', help='ID on s3 for dataset B', required=True)
    parser.add_argument('--s3-directory', '-s', help='source ID on s3 for dataset2', required=True)

    # idea for dataset sources is that all TSV tiles for hadoop will be in a giant S3 dir like this:
    # gadm28__00N_000E.tsv, wdpa__00N_000E.tsv, primary_forest__00N_000E.tsv
    # so the dataset_a and dataset_b strings wil be gadm28, wdpa, primary_forest, etc
    # and the s3_directory will be s3://gfw2-data/alerts-tsv/gadm28-boundary-tsv/ (or something)

    args = parser.parse_args()

    layer_a = Layer(args.dataset_a)
    layer_b = Layer(args.dataset_a)

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






