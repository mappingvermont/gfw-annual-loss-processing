import os
import uuid

from tile import Tile
import util

class Layer(object):

    def __init__(self, source):

        self.source = source
        self.layer_dir = self.create_out_dir()

        self.tile_list = []

        print 'Starting layer class for source {}'.format(self.source)

    def create_out_dir(self):

        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        data_dir = os.path.join(root_dir, 'data')
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)

        guid = str(uuid.uuid4())
        layer_dir = os.path.join(data_dir, guid)
        os.mkdir(layer_dir)

        return layer_dir

    def build_tile_list(self):

        print 'Building tile list'
        print 'checking extent of input geometry {}'.format(self.source)

        # make vrt:
        tiles = 'tiles_over_gadm.shp'
        tiles_layer = tiles.split(".")[0]
        layer_name = self.source.split(".")[0]
        vrt_text = '''<OGRVRTDataSource>
            <OGRVRTLayer name={0}>
            <SrcDataSource>{1}</SrcDataSource>
            <SrcLayer>{0}</SrcLayer>
            </OGRVRTLayer>
            <OGRVRTLayer name={2}>
            <SrcDataSource>{3}</SrcDataSource>
            <SrcLayer>{2}</SrcLayer>
            </OGRVRTLayer>
            </OGRVRTDataSource>'''.format(layer_name, self.source, tiles_layer, tiles)

        with open('data.vrt', 'w') as thefile:
            thefile.write(vrt_text)


        # Copy code from here 1c_Hadoop-Processing/utilities/calc_tsv_extent.py
        sql = "SELECT a.unique_id FROM {0} a, {1} b WHERE ST_Intersects(a.geometry, b.geometry) GROUP BY a.unique_id".format(tiles_layer, layer_name)
        cmd = ['ogrinfo', '-dialect', 'SQLITE',  '-sql', sql, 'data.vrt']
        print cmd

        intersect_tiles = util.run_subprocess(cmd)

        # intersect extent of layer with loss tile footprint

        # then iterate over each tile in the intersected output to figure out possible tiles to make
        # e.g., if there are features in our source that are in the tile's extent

        # then take THAT list (list of tiles where we have data)
        # and create a "tile"
        print intersect_tiles
        print "--------------the end"
        #intersect_tiles = ['00N_110E', '10N_110E', '20N_110']
        for tile_name in intersect_tiles:
            t = Tile(self.source, tile_name, self.layer_dir)

            self.tile_list.append(t)

    def upload_to_s3(self):

        print 'uploading everything in {} to s3'.format(self.layer_dir)

    def download_s3_tile(self, tile_id):

        # download tile from s3 and save to this self.layer_dir

        # then create a tile object
        t = Tile(self.source, tile_id, self.layer_dir)

        # then append it to this layers tile list
        self.tile_list.append(t)








