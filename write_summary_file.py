import os
import argparse
import pandas as pd

from utilities import gain, extent2000, loss, util, postprocess


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
        
    util.prep_output_dirs(output_excel)

    # set default max admin level
    if not args.level:
        if args.iso:
            args.level = 2
        else:
            args.level = 1

    writer = pd.ExcelWriter(output_excel)

    for adm_level in range(0, args.level + 1):

        for output_type in [gain, extent2000, loss]:
            sheet_name, df = output_type.build_df(adm_level, args.iso)

            df.to_excel(writer, sheet_name, index=False)

    writer.save()

    # apply proper formatting, add headers etc
    postprocess.format_excel(output_excel)

if __name__ == '__main__':
    main()

