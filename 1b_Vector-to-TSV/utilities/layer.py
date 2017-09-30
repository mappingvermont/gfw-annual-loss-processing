import os
import uuid

from tile import Tile


class Layer(object):

    def __init__(self, source):

        self.source = source
        self.layer_dir = self.create_out_dir()

        self.tile_list = None

        print 'Starting layer class for source {}'.format(self.source)

    def create_out_dir(self):

        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        data_dir = os.path.join(root_dir, 'data')

        guid = str(uuid.uuid4())
        layer_dir = os.path.join(data_dir, guid)
        os.mkdir(self.layer_dir)

        return layer_dir

    def build_tile_list(self):

        print 'Building tile list'
        print 'checking extent of input geometry {}'.format(self.source)
        # Copy code from here 1c_Hadoop-Processing/utilities/calc_tsv_extent.py

        # intersect extent of layer with loss tile footprint

        # then iterate over each tile in the intersected output to figure out possible tiles to make
        # e.g., if there are features in our source that are in the tile's extent

        # then take THAT list (list of tiles where we have data)
        # and create a "tile"

        # for tile_name in intersect_tiles:
        #     t = Tile(tile_name, self.source, self.layer_dir)
        #
        #     self.tile_list.append(t)

    def upload_to_s3(self):

        print 'uploading everything in {} to s3'.format(self.layer_dir)

    def download_s3_tile(self, tile_id):

        # download tile from s3 and save to this self.layer_dir

        # then create a tile object
        t = Tile(self.source, tile_id, self.layer_dir)

        # then append it to this layers tile list
        self.tile_list.append(t)








