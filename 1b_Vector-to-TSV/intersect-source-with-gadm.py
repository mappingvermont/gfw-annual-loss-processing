import argparse
import os

from utilities.layer import Layer
from utilities import util, geop

from utilities import load_gadm28, postgis_util as pg_util


def main():
    parser = argparse.ArgumentParser(description='Generate Hansen-tiled TSVs for Hadoop PIP')
    parser.add_argument('--input-dataset', '-i', help='the input dataset-- either postgis table or TIF', required=True)
    parser.add_argument('--col-list', '-c', help='columns to include in the output TSV from the source', nargs='+')

    parser.add_argument('--zip-source', '-z', required=True, help='location of zipped gadm-like file on s3')
    parser.add_argument('--output-name', '-n', help='output name to carry forward from ' \
                        'this dataset - will appear in all TSVs', required=True)
    parser.add_argument('--s3-out-dir', '-s', help='s3 out dir', required=True)

    parser.add_argument('--test', dest='test', action='store_true')
    args = parser.parse_args()

    util.start_logging()

    # load source dataset, then create layer object
    source_ext = os.path.splitext(args.input_dataset)[1]

    # raster data
    if source_ext in ['.rvrt', '.tif']:
        input_data = args.input_dataset

    # postgis table
    elif source_ext == '':
        input_data = pg_util.insert_into_postgis(args.input_dataset)

    else:
        raise ValueError('Unexpected extension {}. This process only accepts ' \
                         'tifs and postgis tables'.format(source_ext))

    source_layer = Layer(input_data, args.col_list)

    # load gadm28/custom into postGIS if it doesn't exist already
    gadm28_layer = load_gadm28.load(args.zip_source)

    # create blueprint for the source dataset in postgis
    util.build_gadm28_tile_list(source_layer, args.test)

    if os.path.splitext(args.input_dataset)[1] in ['.rvrt', '.tif']:
        source_layer.raster_to_postgis()

    # process clip jobs above-- this loads tiles into PostGIS
    util.exec_multiprocess(geop.clip, source_layer.tile_list)

    # intersect all tiles of source layer with gadm28
    l = geop.intersect_gadm(source_layer, gadm28_layer)

    l.export(args.output_name)

    l.upload_to_s3(args.s3_out_dir, args.test, False)


if __name__ == '__main__':
    main()
