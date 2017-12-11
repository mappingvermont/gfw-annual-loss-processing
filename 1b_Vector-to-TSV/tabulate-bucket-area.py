import argparse
import logging


from utilities.layer import Layer
from utilities import util, s3_list_tiles, geop


def main():

    parser = argparse.ArgumentParser(description='Tabulate area for a wildcard')
    parser.add_argument('--bucket-path', '-b', help='path to s3 bucket', required=True)
    
    parser.add_argument('--test', dest='test', action='store_true')
    args = parser.parse_args()

    util.start_logging()
    
    util.create_area_table()

    # could be empty, but important for plantations/etc
    # need to register the iso columns as well
    col_list = [{'field_2': 'polyname', 'field_3': 'boundary_field1', 'field_4': 'boundary_field2',
                 'field_5': 'boundary_field3', 'field_6': 'boundary_field4'}]

    iso_col_dict = {'field_7': 'iso', 'field_8': 'id_1', 'field_9': 'id_2'}

    l = Layer(None, col_list[:], iso_col_dict)
    l.batch_download(args.bucket_path)

    # load every tile into PostGIS
    util.exec_multiprocess(geop.clip, l.tile_list, args.test)

    # after all tiles are loaded tabluate each polygons area in aoi_area table
    util.exec_multiprocess(geop.tabulate_area, l.tile_list, args.test)

if __name__ == '__main__':
    main()
