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

    def alias_select_columns(self, tile_alias):

        # remove any iso/id_1/id_2 columns associated; they'll be added separately
        column_names = [x.values()[0] for x in self.col_list]
        select_columns = [x for x in column_names if x not in ['iso', 'id_1', 'id_2']]

        return [tile_alias + '.' + x.lower() for x in select_columns]
