import argparse
from utilities import annual_helpers
from boto.s3.connection import S3Connection


conn = S3Connection(host="s3.amazonaws.com")
bucket = conn.get_bucket('gfw2-data')

#iterate over tsv files, like tsv folder files, and swaths of 00N - 80N 10S-50S... for extent
parser = argparse.ArgumentParser()
parser.add_argument('--analysis-type', '-a', required=True, choices=['extent', 'loss', 'gain', 'biomass'])
parser.add_argument('--extent-folder', '-e', required=False, help='s3 location of extent tsv, either 2000 or 2010')
parser.add_argument('--ouptut-folder', '-o', required=True, help='s3 location for hadoop output')

args = parser.parse_args()
analysis_type = args.analysis_type

# download jar file
annual_helpers.download_jar()

# properties dict
fields_dict= {
              'points_path':{'loss': 'loss_2016/', 'extent': '{}/{}*.tsv', 'biomass': 'biomass_at_30tcd/{}*', 'gain': 'gain_tiles/'}, 
              'points_fields':{'loss': '2,3,4,5', 'extent': '2,3', 'biomass': '2,3'}
              }

if analysis_type in ['extent', 'biomass']:

    ns_list = ['00N', '10N', '20N', '30N', '40N', '50N', '60N', '70N', '80N', '10S', '20S', '30S', '40S', '50S']

    for ns_tile in ns_list:

        # if there is not a csv output already in the file system
        if not annual_helpers.check_output_exists(analysis_type, ns_tile, args.extent_folder):

            annual_helpers.write_props(analysis_type, fields_dict, ns_tile)

            annual_helpers.call_pip()

            annual_helpers.upload_to_s3(analysis_type, ns_tile)

else:

    # if there is not a csv output already in the file system
    if not annual_helpers.check_output_exists(analysis_type):
    
        points_path = fields_dict['points_path'][analysis_type]

        annual_helpers.write_props(analysis_type, fields_dict)

        annual_helpers.call_pip()

        annual_helpers.upload_to_s3(analysis_type)
