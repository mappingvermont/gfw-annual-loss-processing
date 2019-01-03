import argparse
import subprocess
import glob
import multiprocessing
import boto

from utilities.layer import Layer
from utilities import util, s3_list_tiles, geop, postgis_util
from utilities import AOI_to_tsv

def main():

    parser = argparse.ArgumentParser(description='Intersect two pre-tiled datasets to create union tiles')
    parser.add_argument('--input', '-i', help='directory of input AOIs', required=True)

    parser.add_argument('--s3-out-dir', '-s', help='s3 out dir', required=True)

    args = parser.parse_args()

    cmd = ['aws', 's3', 'cp', args.input, '.']
    print cmd
    subprocess.check_call(cmd)

    all_shp = glob.glob('*.shp')
    print all_shp

    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=count/3)
    pool.map(AOI_to_tsv.convert_AOI, all_shp)
    pool.close()
    pool.join()

    cmd = ['aws', 's3', 'cp', '.', args.s, '--recursive']
    subprocess.check_call(cmd)



if __name__ == '__main__':
    main()