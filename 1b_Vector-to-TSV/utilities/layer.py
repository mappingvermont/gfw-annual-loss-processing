import os
import uuid
import fiona 
from shapely.geometry import shape

from tile import Tile

class Layer(object):

    def __init__(self, source, col_list):

        self.source = source
        self.col_list = col_list
        self.layer_dir = self.create_out_dir()

        self.tile_list = []

        self.build_col_list()

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

    def build_col_list(self):

        # if none specified, build dummy list
        if not self.col_list:
            self.col_list = ['boundary_field1', 'boundary_field2']

        elif len(self.col_list) > 2:
            raise ValueError('Can only save 2 or fewer columns from this dataset')

        # if len(col_list) is 0 or 1, fill empty space with dummy value of '1'
        elif len(self.col_list) == 1:
            self.col_list += ['boundary_field2']

    def build_tile_list(self):

        print 'Building tile list'
        print 'checking extent of input geometry {}'.format(self.source)
        
        # shapefile of tiles used to tsv aoi
        # tiles = fiona.open(os.path.join('grid', 'footprint_1degree.shp'), 'r')
        tiles = fiona.open(os.path.join('grid', 'lossdata_footprint_filter.geojson'), 'r')

        # aoi we want to tsv (take this out)
        aoi = fiona.open(self.source)
    
        # select tiles that are inside of the bounding box of the aoi
        tiles_in_aoi = tiles.filter(bbox=(aoi.bounds))

        for feat in tiles_in_aoi:
            # get the bounding box of the 1deg tile
            bbox = shape(feat['geometry']).bounds 
            
            # get the tile id- used for naming
            # tile_id = feat['properties']['ulx_uly']
            tile_id = feat['properties']['ID']

            # build the tile object
            t = Tile(self.source, self.col_list, tile_id, bbox, self.layer_dir)

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
