import argparse
import os

from utilities.layer import Layer
from utilities import util, geop, load_gadm28

from utilities import load_gadm28


def main():
    parser = argparse.ArgumentParser(description='Generate Hansen-tiled TSVs for Hadoop PIP')
    parser.add_argument('--input-dataset', '-i', help='the input dataset', required=True)
    parser.add_argument('--col-dict', '-c', help='columns to include in the output TSV from the source', nargs='+')

    parser.add_argument('--zip-source', '-z', help='if not intersecting with gadm28, location of zipped file on s3, matches gadm28 schema')
    parser.add_argument('--unzipname', '-u', help='if not intersecting with gadm28, name of unzipped file')

    parser.add_argument('--output-format', '-o', help='output format', default='tsv', choices=('tsv', 'shp', 'geojson'))
    parser.add_argument('--output-name', '-n', help='output name', required=True)
    parser.add_argument('--s3-out-dir', '-s', help='s3 out dir', default='s3://gfw2-data/alerts-tsv/tsv-boundaries-tiled/')

    parser.add_argument('--test', dest='test', action='store_true')

    args = parser.parse_args()

    util.start_logging()

    # create layer object
    source_layer = Layer(args.input_dataset, args.col_dict)

    # load gadm28 into postGIS if it doesn't exist already
    gadm28_layer = load_gadm28.load(args.zip_source, args.unzipname)
    
    # create blueprint for the source dataset in postgis
    util.build_gadm28_tile_list(source_layer, args.test)

    if os.path.splitext(args.input_dataset)[1] == '.tif':
        source_layer.raster_to_vector()

    # process clip jobs above-- this loads tiles into PostGIS
    util.exec_multiprocess(geop.clip, source_layer.tile_list)

    # intersect all tiles of source layer with gadm28
    l = geop.intersect_gadm(source_layer, gadm28_layer)

    l.export(args.output_name, args.output_format)

    l.upload_to_s3(args.output_format, args.s3_out_dir, args.test, False)


if __name__ == '__main__':
    main()
