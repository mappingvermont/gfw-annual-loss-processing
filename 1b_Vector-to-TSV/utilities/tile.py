import os


class Tile(object):

    def __init__(self, dataset, col_list, tile_id, bbox, postgis_table=None):

        self.dataset = dataset
        self.col_list = col_list
        self.tile_id = tile_id
        self.bbox = bbox
        self.postgis_table = postgis_table

        self.final_output = None

        if self.dataset:
            self.dataset_name = os.path.splitext(os.path.basename(self.dataset))[0]
        else:
            self.dataset_name = None

    def create_plain_tile(self):

        # use ogr2ogr to do extract tile from bounding box using -spat flag
        # and save to that crazy TSV format
        # example ogr2ogr statement here:
        # https://github.com/wri/raster-vector-to-tsv/blob/master/processing/vector/tile_vector.py#L42-L43

        print 'creating plain tile {} for {}, extent'.format(self.tile_id, self.dataset, ', '.join(self.bbox))

    def alias_columns(self, tile_alias):

        return [tile_alias + '.' + x.lower() for x in self.col_list]
