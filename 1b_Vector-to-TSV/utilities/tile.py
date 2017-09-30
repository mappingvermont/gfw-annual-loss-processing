import os


class Tile(object):

    def __init__(self, dataset, tile_id, out_dir):

        self.tile_id = tile_id
        self.dataset = dataset

        dataset_name = os.path.splitext(os.path.basename(dataset))[0]
        output_name = '{}__{}.tsv'.format(dataset_name, tile_id)

        self.output_file = os.path.join(out_dir, output_name)

        self.bbox = self.bbox_from_tile_id()

        print 'Creating tile {} for geometry {}'.format(self.tile_id, self.dataset)

    def bbox_from_tile_id(self):

        print 'Looking up bbox for tile {}'.format(self.tile_id)

        return [-90, 10, -80, 0]

    def intersect_with_gadm28(self):

        print 'intersecting tile {}, geometry {} with gadm28'.format(self.tile_id, self.dataset)

        # download the correct gadm28 tile
        # write VRT so it reads gadm28 TSV correctly
        # intersect

    def create_plain_tile(self):

        # use ogr2ogr to do extract tile from bounding box using -spat flag
        # and save to that crazy TSV format
        # example ogr2ogr statement here:
        # https://github.com/wri/raster-vector-to-tsv/blob/master/processing/vector/tile_vector.py#L42-L43

        print 'creating plain tile {} for {}, extent'.format(self.tile_id, self.dataset, ', '.join(self.bbox))











