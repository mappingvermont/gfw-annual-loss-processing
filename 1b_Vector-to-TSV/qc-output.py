import argparse

import psycopg2

from utilities import util, s3_list_tiles, zstats, load_gadm28, qc_util as qc, postgis_util as pg_util


def main():
    parser = argparse.ArgumentParser(description='QC loss and extent stats')
    parser.add_argument('--number-of-tiles', '-n', help='number of tiles to run QC', default=10, type=int, required=True)
    parser.add_argument('--grid-resolution', '-g', help='grid resolution of source', type=int, default=10, choices=(10, 0.25), required=True)

    parser.add_argument('--s3-poly-dir', '-s', help='input poly directory for intersected TSVs', required=True)
    parser.add_argument('--test', dest='test', action='store_true')

    args = parser.parse_args()
    util.start_logging()

    # load gadm28 - required to determine which gadm28 boundaries are completely within a 10x10 tile
    load_gadm28.load('s3://gfw2-data/alerts-tsv/gis_source/adm2_final.zip')

    if args.test:
        args.number_of_tiles = 1

    tile_list = s3_list_tiles.pull_random(args.s3_poly_dir, args.number_of_tiles)
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

        # make sure GPD was able to read the geojson successfully
        # if not will log an error and finish the task, so the whole
        # process can continue
        if not gdf.empty:
		local_geojson = util.dissolve_tsv(gdf, local_geojson)

		# sample geojson to take only 100 features
		# some combinations have 1000+ - would take forever
		local_geojson = util.subset_geojson(local_geojson, 100)

		valid_admin_list = qc.filter_valid_adm2_boundaries(tile_name)

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

