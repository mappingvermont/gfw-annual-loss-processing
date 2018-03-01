import argparse
from utilities import annual_helpers
from boto.s3.connection import S3Connection


conn = S3Connection(host="s3.amazonaws.com")
bucket = conn.get_bucket('gfw2-data')

#iterate over tsv files, like tsv folder files, and swaths of 00N - 80N 10S-50S... for extent
parser = argparse.ArgumentParser()
parser.add_argument('--analysis-type', '-a', required=True, choices=['extent', 'loss', 'gain', 'biomass'])
parser.add_argument('--points-folder', '-t', required=True, help='s3 location of points folder')
parser.add_argument('--polygons-folder', '-y', required=True, help='s3 location of polygons folder')
parser.add_argument('--output-folder', '-o', required=True, help='s3 location for hadoop output')
parser.add_argument('--dryrun', '-d', action='store_true', help="only write application.properties, don't call pip")

args = parser.parse_args()
analysis_type = args.analysis_type

# download jar file
annual_helpers.download_jar()

# properties dict
points_fields_dict = {'loss': '2,3,4,5', 'extent': '2,3', 'biomass': '2,3'}

if analysis_type in ['extent', 'biomass']:

    ns_list = ['00N', '10N', '20N', '30N', '40N', '50N', '60N', '70N', '80N', '10S', '20S', '30S', '40S', '50S']

    for ns_tile in ns_list:

        # if there is not a csv output already in the file system
        if not annual_helpers.check_output_exists(analysis_type, args.output_folder, ns_tile):
            print "does not already exist"

            annual_helpers.write_props(analysis_type, points_fields_dict, args.points_folder, args.polygons_folder, ns_tile)

            annual_helpers.call_pip(args.dryrun)

            annual_helpers.upload_to_s3(analysis_type, args.output_folder, args.dryrun, ns_tile)

else:

    # if there is not a csv output already in the file system
    if not annual_helpers.check_output_exists(analysis_type, args.output_folder):

        annual_helpers.write_props(analysis_type, points_fields_dict, args.points_folder, args.polygons_folder)

        annual_helpers.call_pip(args.dryrun)

        annual_helpers.upload_to_s3(analysis_type, args.output_folder, args.dryrun)
