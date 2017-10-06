import os
import uuid
import fiona 
from shapely.geometry import shape, mapping

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
        
        # shapefile of tiles used to tsv aoi
        tiles = fiona.open(r'grid\footprint_1degree.shp', 'r')

        # aoi we want to tsv (take this out)
        aoi = fiona.open(self.source)
    
        # select tiles that are inside of the bounding box of the aoi
        tiles_in_aoi = tiles.filter(bbox = (aoi.bounds))

        for feat in tiles_in_aoi:
            # get the bounding box of the 1deg tile
            bbox = shape(feat['geometry']).bounds 
            
            # get the tile id- used for naming
            tile_id = feat['properties']['ulx_uly']
            
            # build the tile object
            t = Tile(self.source, tile_id, bbox, self.layer_dir)

            # add the tile bbox to the tile_list
            self.tile_list.append(t)
                   
        
    def upload_to_s3(self):

        print 'uploading everything in {} to s3'.format(self.layer_dir)

    def download_s3_tile(self, tile_id):

        # download tile from s3 and save to this self.layer_dir

        # then create a tile object
        t = Tile(self.source, tile_id, self.layer_dir)

        # then append it to this layers tile list
        self.tile_list.append(t)
