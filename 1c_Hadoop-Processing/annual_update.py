import argparse
from utilities import annual_helpers
from boto.s3.connection import S3Connection
import sys

# conn = S3Connection(host="s3.amazonaws.com")
# bucket = conn.get_bucket('gfw2-data')

#iterate over tsv files, like tsv folder files, and swaths of 00N - 80N 10S-50S... for extent
parser = argparse.ArgumentParser()
parser.add_argument('--analysis-type', '-a', required=True, choices=['extent', 'loss', 'gain', 'biomass',
                                                                     'grossEmis', 'netEmis', 'cumulGain', 'annualGain'])
parser.add_argument('--points-folder', '-t', required=True, help='s3 location of points folder')
parser.add_argument('--polygons-folder', '-y', required=True, help='s3 location of polygons folder')
parser.add_argument('--output-folder', '-o', required=True, help='s3 location for hadoop output')
parser.add_argument('--dryrun', '-d', action='store_true', help="only write application.properties, don't call pip")
parser.add_argument('--iterate-by', '-i', nargs='+', required=True, choices=['points', 'polygons', 'None'])


def main():

    args = parser.parse_args()
    iterate_by =  args.iterate_by
    analysis_type = args.analysis_type

    # download jar file
    annual_helpers.download_jar(args.dryrun)

    # properties dict
    points_fields_dict = {'loss': '2,3,4,5', 'extent': '2,3', 'biomass': '2,3', 'gain': '2,3',
                          'grossEmis': '2,3,4,5', 'netEmis': '2,3,4', 'cumulGain': '2,3,4', 'annualGain': '2,3,4'}

    if 'None' in iterate_by and len(iterate_by) != 1:
        print "Either specify None OR an iterate-by option"

    if 'None' in iterate_by:
        iterate_by = False

    if iterate_by and (analysis_type == 'loss' or analysis_type == 'gain'):
        raise ValueError("If running loss or gain, set iterate-by to None")

    if iterate_by:
        ns_list = ['00N', '10N', '20N', '30N', '40N', '50N', '60N', '70N', '80N', '10S', '20S', '30S', '40S', '50S']

        for ns_tile in ns_list:
            write_props_and_run(analysis_type, args, points_fields_dict, ns_tile)

    else:
        write_props_and_run(analysis_type, args, points_fields_dict)


def write_props_and_run(analysis_type, args, points_fields_dict, ns_tile=None):

    if not annual_helpers.check_output_exists(args, ns_tile):

        annual_helpers.write_props(args, points_fields_dict, ns_tile)

        annual_helpers.call_pip(args.dryrun)

        annual_helpers.upload_to_s3(analysis_type, args.output_folder, args.dryrun, ns_tile)


if __name__ == '__main__':
    main()
