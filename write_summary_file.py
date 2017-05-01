import os
import argparse
import pandas as pd
from openpyxl import load_workbook

from utilities import gain, extent2000, loss, util


def main():

    parser = argparse.ArgumentParser(description='Summarize hadoop output data in pretty pivot tables')
    parser.add_argument('--iso', '-i', help='Select an ISO code to process')
    parser.add_argument('--level', '-l', help='Max admin level to summarize')
    args = parser.parse_args()

    root_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(root_dir, 'output')

    util.validate_input_data(root_dir)

    if args.iso:
        args.iso = args.iso.upper()
        output_excel = os.path.join(output_dir, 'tree_cover_stats_2015_{}.xlsx'.format(args.iso))

    else:
        output_excel = os.path.join(output_dir, 'tree_cover_stats_2015.xlsx')
        
    # grab the read me from the excel template to start our new workbook
    # return the wb object so we can write to it
    excel_template = os.path.join(root_dir, 'example_output.xlsx')
    wb = util.prep_output_file(excel_template, output_excel, args.iso)

    # set default max admin level
    if not args.level:
        if args.iso:
            args.level = 2
        else:
            args.level = 1

    # open our workbook for writing and add various sheets
    with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
        writer.book = wb

        for output_type in [extent2000, loss, gain]:
            for adm_level in range(0, args.level + 1):
                sheet_name, df = output_type.build_df(adm_level, args.iso)

                df.to_excel(writer, sheet_name)


if __name__ == '__main__':
    main()

