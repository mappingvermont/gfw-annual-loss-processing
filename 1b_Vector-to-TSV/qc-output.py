import argparse

from utilities import util, s3_list_tiles, zstats, qc_util as qc


def main():
    parser = argparse.ArgumentParser(description='QC loss and extent stats')
    parser.add_argument('--number-of-tiles', '-n', help='number of tiles to run QC', default=10, type=int, required=True)
    parser.add_argument('--grid-resolution', '-g', help='grid resolution of source', type=int, default=10, choices=(10, 0.25), required=True)

    parser.add_argument('--s3-poly-dir', '-s', help='input poly directory for intersected TSVs', required=True)
    parser.add_argument('--output-dataset-id', '-o', help='the output dataset in the API to compare results', required=True)

    parser.add_argument('--test', dest='test', action='store_true')

    args = parser.parse_args()
    util.start_logging()

    tile_list = s3_list_tiles.pull_random(args.s3_poly_dir, args.number_of_tiles)
    tile_list = ['ifl_2013__gfw_manged_forests__50N_090W.tsv']
    print tile_list

    temp_dir = util.create_temp_dir()

    # need to make a layer dir for this . . . abstract to util module
    qc_output_tile(tile_list[0], args.s3_poly_dir, temp_dir)


def qc_output_tile(tile_name, s3_src_dir, temp_dir):

    local_geojson = util.s3_to_dissolved_geojson(s3_src_dir, tile_name, temp_dir)

    valid_admin_list = qc.filter_valid_adm2_boundaries(tile_name)

    df = zstats.calc_api(local_geojson, valid_admin_list)

    joined = qc.join_to_api_df(df)

    qc.compare_outputs(joined)


if __name__ == '__main__':
    main()
