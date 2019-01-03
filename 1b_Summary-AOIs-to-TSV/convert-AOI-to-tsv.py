import argparse
import subprocess
import glob
from multiprocessing.pool import Pool
from functools import partial

from utilities.layer import Layer
from utilities import util, s3_list_tiles, geop, postgis_util
from utilities import AOI_to_tsv

def main():

    parser = argparse.ArgumentParser(description='Convert a list of shapefiles into tsvs without intersecting them with GADM')
    parser.add_argument('--input', '-i', help='directory of input AOIs', required=True)
    parser.add_argument('--name', '-n', help='shapefile field with name of AOI', required=True)
    parser.add_argument('--s3-out-dir', '-s', help='s3 out dir', required=True)

    args = parser.parse_args()

    cmd = ['aws', 's3', 'cp', args.i, '.', '--recursive']
    subprocess.check_call(cmd)

    all_shp = glob.glob('*.shp')
    print all_shp

    name_field = args.m

    num_of_processes = 20
    pool = Pool(num_of_processes)
    pool.map(partial(AOI_to_tsv.convert_AOI, name_field=name_field), all_shp)
    pool.close()
    pool.join()

    cmd = ['aws', 's3', 'cp', '.', args.s, '--recursive']
    subprocess.check_call(cmd)



if __name__ == '__main__':
    main()