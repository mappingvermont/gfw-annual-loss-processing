import argparse

from utilities.layer import Layer
from utilities import util, s3_list_tiles, geop, postgis_util


def main():

    parser = argparse.ArgumentParser(description='Intersect two pre-tiled datasets to create union tiles')
    parser.add_argument('--dataset-a', '-a', help='s3 path (with wildcard) to dataset A', required=True)
    parser.add_argument('--dataset-b', '-b', help='s3 path (with wildcard) to dataset B', required=True)

    parser.add_argument('--output-format', '-o', help='output format', default='tsv', choices=('tsv', 'shp', 'geojson'))
    parser.add_argument('--output-name', '-n', help='output name', required=True)

    parser.add_argument('--root-s3-dir', '-r', help='root s3 dir', required=True)
    parser.add_argument('--s3-out-dir', '-s', help='s3 out dir', required=True)

    parser.add_argument('--batch', dest='batch', action='store_true')
    parser.add_argument('--test', dest='test', action='store_true')
    args = parser.parse_args()

    util.start_logging()

    # connect to postgis to make sure database is tuned properly
    # could do this later, but better now before we start multithreading
    conn, cursor = postgis_util.conn_to_postgis()

    # these boundary fields already exist in the TSV
    # may be empty, but could contain stuff like plantation type/species, etc
    col_list = [{'field_3': 'boundary_field1'}, {'field_4': 'boundary_field2'}]

    # include reference to iso/id_1/id_2 fields
    iso_col_dict = {'field_7': 'iso', 'field_8': 'id_1', 'field_9': 'id_2'}

    # create a layer for each dataset
    # assume layer_a and b both have iso cols; only need to pass it to one
    layer_a = Layer(args.dataset_a, col_list[:], iso_col_dict)
    layer_b = Layer(args.dataset_b, col_list[:])

    # If we're working with smaller grids (0.25 x 0.25 degrees),
    # download all tiles from s3 that match the dataset name supplied
    if args.batch:

        for l in [layer_a, layer_b]:
            l.batch_download(args.root_s3_dir)

        # from the local tiles downloaded, find overlap between the two layers
        s3_list_tiles.find_local_overlap(layer_a, layer_b)

    else:
        # figure out what tiles they have in common by looking at datasets on S3
        overlap_list = s3_list_tiles.find_tile_overlap(args.dataset_a, args.dataset_b, args.root_s3_dir, args.test)

        # download all tiles in common, build layer.tile_list for each layer
        for tile_id in overlap_list:
            layer_a.download_s3_tile(args.dataset_a, args.root_s3_dir, tile_id)
            layer_b.download_s3_tile(args.dataset_b, args.root_s3_dir, tile_id)

    # load every tile into PostGIS
    for vrt_tile in [layer_a.tile_list + layer_b.tile_list]:
        util.exec_multiprocess(geop.clip, vrt_tile, args.test)

    # create new layer based on this intersection
    layer_c = geop.intersect_layers(layer_a, layer_b)

    # export to TSV
    layer_c.export(args.output_name, args.output_format)

    # upload output
    layer_c.upload_to_s3(args.output_format, args.s3_out_dir, args.test, args.batch)


if __name__ == '__main__':
    main()
