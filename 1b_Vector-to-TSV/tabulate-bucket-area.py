import argparse

import pandas as pd

from utilities.layer import Layer
from utilities import util, geop, postgis_util as pg_util


def main():

    parser = argparse.ArgumentParser(description='Tabulate area for a wildcard')
    parser.add_argument('--bucket-path', '-b', help='path to s3 bucket', required=True)

    parser.add_argument('--test', dest='test', action='store_true')
    args = parser.parse_args()

    util.start_logging()

    pg_util.create_area_table()

    # could be empty, but important for plantations/etc
    # need to register the iso columns as well
    col_list = [{'field_2': 'polyname', 'field_3': 'bound1', 'field_4': 'bound2',
                 'field_5': 'bound3', 'field_6': 'bound4'}]

    iso_col_dict = {'field_7': 'iso', 'field_8': 'id_1', 'field_9': 'id_2'}

    l = Layer(None, col_list[:], iso_col_dict)
    l.batch_download(args.bucket_path)

    # load every tile into PostGIS
    util.exec_multiprocess(geop.clip, l.tile_list, args.test)

    # after all tiles are loaded tabluate each polygons area in aoi_area table
    util.exec_multiprocess(geop.tabulate_area, l.tile_list, args.test)

    # grab final area_aoi table from postgres
    engine = pg_util.sqlalchemy_engine()
    df = pd.read_sql('SELECT * FROM aoi_area', con=engine)

    # fill nan with -9999 or pandas groupby won't work properly
    df = df.fillna(-9999)

    # given likely overlap of iso/adm1/adm2 across tiles, need to group
    groupby_cols = list(df.columns[:-1])
    area_column = 'area_poly_aoi'

    grouped = df.groupby(groupby_cols)[area_column].sum().reset_index()
    grouped = grouped.rename(columns={'id_1': 'adm1', 'id_2': 'adm2'})

    output_file = 'area.csv'
    print 'Area CSV saved here: {}'.format(output_file)
    grouped.to_csv(output_file, index=False, encoding='utf-8')

if __name__ == '__main__':
    main()
