import argparse
import subprocess
import glob
from multiprocessing.pool import Pool
from functools import partial

from utilities import AOI_to_tsv

def main():

    parser = argparse.ArgumentParser(description='Convert a list of shapefiles into tsvs without intersecting them with GADM')
    parser.add_argument('--input', '-i', help='directory of input AOIs', required=True)
    parser.add_argument('--name', '-n', help='shapefile field with name of AOI', required=True)
    parser.add_argument('--s3-out-dir', '-s', help='s3 out dir', required=True)

    args = parser.parse_args()

    # # Copies all the shapefiles from the input s3 folder to the spot machine
    # cmd = ['aws', 's3', 'cp', args.input, '.', '--exclude', '*.tif*', '--recursive']
    # subprocess.check_call(cmd)

    # Makes a list of all the shapefiles
    all_shp = glob.glob('*.shp')
    print "List of shapefiles to be processed:", all_shp

    # The field in the shapefiles with the name you want to use for the tsv
    name_field = args.name

    # num_of_processes = 20
    # pool = Pool(num_of_processes)
    # pool.map(partial(AOI_to_tsv.shp_to_csv, name_field=name_field), all_shp)
    # pool.close()
    # pool.join()

    # # For testing with a single processor
    # for shp in all_shp:
    #     AOI_to_tsv.shp_to_csv(shp, name_field)

    # For testing with a single processor
    for shp in all_shp:
        AOI_to_tsv.csv_to_tsv(shp)

    # Copies tsvs to s3
    cmd = ['aws', 's3', 'cp', '.', args.s3_out_dir, '--exclude', '*', '--include', '*.tsv', '--recursive']
    subprocess.check_call(cmd)



if __name__ == '__main__':
    main()