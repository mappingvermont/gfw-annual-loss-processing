import os
import subprocess
import boto3
from urlparse import urlparse
import logging

from tile import Tile
import geop, util, decode_polygon_tsv, export


class Layer(object):

    def __init__(self, input_dataset, col_list, iso_col_dict=None):

        logging.info('Starting layer class for source {}'.format(input_dataset))

        self.input_dataset = input_dataset
        self.check_layer_source()

        self.col_list = col_list
        self.iso_col_dict = iso_col_dict

        self.build_col_list()
        self.layer_dir = None

        self.layer_dir = util.create_temp_dir()

        self.tile_list = []

    def check_layer_source(self):

        # some layers don't have an input dataset
        # (those created from an intersection of two input layers)
        # if they do have one, make sure that the input format is correct
        if self.input_dataset:
            src_ext = os.path.splitext(self.input_dataset)[1]

	    if src_ext not in ['.rvrt', '.tif', '']:
		raise ValueError('Unexpected extension {}. This process only accepts ' \
				 'tifs and postgis tables'.format(source_ext))

    def build_col_list(self):

        # if none specified, build dummy list
        if not self.col_list:
            self.col_list = [{'1': 'bound1'}, {'1': 'bound2'}]

        elif self.input_dataset and len(self.col_list) > 2:
            logging.error(self.col_list)
            raise ValueError('Can only save 2 or fewer columns from this dataset')

        # for command line args, just want to pass in column names
        # don't care what boundary_field name they're aliased to
        elif isinstance(self.col_list[0], str):

            output_list = []

            for i, fieldname in enumerate(self.col_list):
                boundary_fieldname = 'bound{}'.format(str(i + 1))

                output_list.append({fieldname: boundary_fieldname})

            self.col_list = output_list

        # if len(col_list) is 1, fill empty space with dummy value of '1'
        if len(self.col_list) == 1:
            self.col_list.append({'1': 'bound2'})

        # if we're passing in ISO information, add this as well
        # important for when we're intersecting two pre-tiled datasets
        if self.iso_col_dict:
            for k, v in self.iso_col_dict.iteritems():
                self.col_list.append({k: v})

    def raster_to_postgis(self):
        geop.raster_to_postgis(self.layer_dir, self.tile_list)

    def upload_to_s3(self, s3_out_dir, is_test, batch_upload):

        logging.info('uploading {} to {}'.format(self.layer_dir, s3_out_dir))

        if batch_upload:
            cmd = ['aws', 's3', 'cp', '--recursive', self.layer_dir, s3_out_dir, '--exclude', '*', '--include', '*.tsv']

            if is_test:
                cmd += '--dryrun'

            subprocess.check_call(cmd)

        else:

    	    # check to make sure we've written out some data
    	    out_tsv_list = [x.final_output for x in self.tile_list if x.final_output and os.stat(x.final_output).st_size]

    	    for out_tsv in out_tsv_list:
        		cmd = ['aws', 's3', 'cp', out_tsv, s3_out_dir]

        		if is_test:
        		    cmd += ['--dryrun']

        		subprocess.check_call(cmd)


    def download_s3_tile(self, dataset_name, s3_path, tile_id):

        s3 = boto3.resource('s3')
        parsed = urlparse(s3_path)
        bucket = parsed.netloc

        tsv_name = tile_id + '.tsv'

        s3_path = '{}{}__{}'.format(parsed.path[1:], dataset_name, tsv_name)
        output_path = os.path.join(self.layer_dir, tsv_name)

        logging.info('Downloading {}__{}'.format(dataset_name, tsv_name))

        s3.Bucket(bucket).download_file(s3_path, output_path)
        vrt_path = os.path.splitext(output_path)[0] + '.vrt'

        tile_vrt = decode_polygon_tsv.build_vrt(output_path, vrt_path)
        postgis_table = '_'.join([dataset_name, tile_id]).lower()

        # then create a tile object
        t = Tile(tile_vrt, self.col_list, tile_id, None, postgis_table)

        # then append it to this layers tile list
        self.tile_list.append(t)

    def batch_download(self, s3_root_dir):

        if self.input_dataset:
            wildcard = '{}*'.format(self.input_dataset)
        else:
            wildcard = '*'

        cmd = ['aws', 's3', 'cp', '--recursive', s3_root_dir, self.layer_dir,
               '--exclude', '*', '--include', wildcard]

        subprocess.check_call(cmd)

        for f in os.listdir(self.layer_dir):

            if os.path.splitext(f)[1] == '.tsv':

                vrt_path = os.path.join(self.layer_dir, os.path.splitext(f)[0] + '.vrt')
                tile_vrt = decode_polygon_tsv.build_vrt(os.path.join(self.layer_dir, f), vrt_path)

                basename = os.path.splitext(os.path.basename(f))[0]
                tile_id = basename.split('__')[-1]

                # required for use case where we need to download all
                # the tiles in an s3 dir and tabulate area
                if self.input_dataset:
                    src_table_basename = self.input_dataset
                else:
                    src_table_basename = '__'.join(basename.split('__')[:-1])

                postgis_table = '_'.join([src_table_basename, tile_id]).lower()

                t = Tile(tile_vrt, self.col_list, tile_id, None, postgis_table)

                self.tile_list.append(t)

    def export(self, output_name):

        input_list = []

        for t in self.tile_list:
            input_list.append((self.layer_dir, output_name, t))

        util.exec_multiprocess(export.export, input_list)
