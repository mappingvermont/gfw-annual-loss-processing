import os
import uuid
import subprocess

from tile import Tile
import geop


class Layer(object):

    def __init__(self, input_dataset, col_list):

        self.input_dataset = input_dataset
        self.col_list = col_list

        self.build_col_list()
        self.layer_dir = None

        self.create_out_dir()

        self.tile_list = []

        print 'Starting layer class for source {}'.format(self.input_dataset)

    def create_out_dir(self):

        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        data_dir = os.path.join(root_dir, 'data')
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)

        guid = str(uuid.uuid4())
        layer_dir = os.path.join(data_dir, guid)
        os.mkdir(layer_dir)

        self.layer_dir = layer_dir

    def build_col_list(self):

        # if none specified, build dummy list
        if not self.col_list:
            self.col_list = ['boundary_field1', 'boundary_field2']

        elif len(self.col_list) > 2:
            raise ValueError('Can only save 2 or fewer columns from this dataset')

        # if len(col_list) is 0 or 1, fill empty space with dummy value of '1'
        elif len(self.col_list) == 1:
            self.col_list += ['boundary_field2']

    def upload_to_s3(self, output_format, s3_out_dir):

        if output_format == 'tsv':

            print 'uploading {} to {}'.format(self.layer_dir, s3_out_dir)
            for t in self.tile_list:

                cmd = ['aws', 's3', 'cp', t.final_output, s3_out_dir, '--dryrun']
                subprocess.check_call(cmd)

        else:
            print 'Output format is {}, not uploading anything to s3'

    def download_s3_tile(self, tile_id):

        # download tile from s3 and save to this self.layer_dir

        # then create a tile object
        t = Tile(self.input_dataset, tile_id, self.layer_dir)

        # then append it to this layers tile list
        self.tile_list.append(t)

    def export(self, output_name, output_format):

        for t in self.tile_list:
            geop.export(self.layer_dir, output_name, t, output_format)

