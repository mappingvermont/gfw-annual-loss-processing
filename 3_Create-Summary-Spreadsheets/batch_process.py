import os
import argparse
import pandas as pd
import openpyxl

from utilities import util

parser = argparse.ArgumentParser(description='Batch process all countries')
parser.add_argument('--type', '-t', choices=('raw', 'postprocess'), required=True)
args = parser.parse_args()

conn = util.db_connect()
cursor = conn.cursor()

sql = 'SELECT iso FROM adm_lkp GROUP BY iso'
all_iso_codes = [c[0] for c in cursor.execute(sql).fetchall()]

root_dir = os.path.dirname(os.path.realpath(__file__))

for iso in all_iso_codes:
    print 'starting to write file for {}'.format(iso)

    if args.type == 'raw':
        from write_summary_file import write_output

        pandas_version = int(pd.__version__.split('.')[1])
        if pandas_version > 16:
            raise ValueError('Pandas version must be less than or equal to 16 to output correct excel sheet')

        write_output(iso, level=None)

    else:
        from postprocess import format_excel

        openpyxl_version = int(''.join(openpyxl.__version__.split('.')[0:2]))
        if openpyxl_version < 24:
            raise ValueError('openpyxl must be version 2.4.x or greater when postprocessing')

        output_excel = os.path.join(root_dir, 'output', 'tree_cover_stats_2017_{}.xlsx'.format(iso))
        format_excel(output_excel, None)


    

