import argparse

import psycopg2

from utilities import util, s3_list_tiles, zstats, qc_util as qc, postgis_util as pg_util


def main():
    parser = argparse.ArgumentParser(description='QC loss and extent stats')
    parser.add_argument('--number-of-tiles', '-n', help='number of tiles to run QC', default=10, type=int, required=True)
    parser.add_argument('--grid-resolution', '-g', help='grid resolution of source', type=int, default=10, choices=(10, 0.25), required=True)

    parser.add_argument('--s3-poly-dir', '-s', help='input poly directory for intersected TSVs', required=True)
    parser.add_argument('--test', dest='test', action='store_true')

    args = parser.parse_args()
    util.start_logging()

    tile_list = s3_list_tiles.pull_random(args.s3_poly_dir, args.number_of_tiles)
    tile_list = [u'bra_biomes__10S_050W.tsv', u'gfw_manged_forests__10N_010E.tsv', u'wdpa_diss_all__10N_030E.tsv']
    print tile_list

    temp_dir = util.create_temp_dir()

    process_list = []

    for tile in tile_list:
        process_list.append((tile, args.s3_poly_dir, temp_dir))

    util.exec_multiprocess(qc_output_tile, process_list, args.test)

    qc.check_results()


def qc_output_tile(q):
    while True:
        tile_name, s3_src_dir, temp_dir = q.get()

        gdf, local_geojson = util.s3_to_gdf(s3_src_dir, tile_name, temp_dir)

        local_geojson = util.dissolve_tsv(gdf, local_geojson)

        valid_admin_list = qc.filter_valid_adm2_boundaries(tile_name)
        print valid_admin_list

        # make sure that dissolve was successful hitting the API
        if local_geojson:
            df = zstats.calc_api(local_geojson, valid_admin_list)
        else:
            df = pd.DataFrame()
        
        if df.empty: 
            print 'No overlap between API values and valid admin list'
        
        else:
            joined = qc.join_to_api_df(df)

            qc.compare_outputs(joined)

        q.task_done()


if __name__ == '__main__':
    main()

