import shutil
import os
import subprocess
import sys
from urlparse import urlparse
from boto.s3.connection import S3Connection




def download_jar(dryrun):

    # check first to see if the target folder is already there:
    if not os.path.exists('target') and not dryrun:

        jar_file = 's3://gfw2-data/alerts-tsv/target_0.3.zip'

        cmd = ['aws', 's3', 'cp', jar_file, '.']
        subprocess.check_call(cmd)

        jar_name = os.path.basename(jar_file)
        cmd = ['unzip', jar_name]
        subprocess.check_call(cmd)


def write_props(args, points_fields_dict, ns_tile=None):

    analysis_type = args.analysis_type
    points_folder = args.points_folder
    polygons_folder = args.polygons_folder
    iterate_by = args.iterate_by

    # for our purposes, extent is the same as gain
    # four input fields (x, y, value and area)
    # and this is easier than editing the scala code to include a gain type
    if iterate_by:

        # biomass and extent are run in bands of 10 degree latitudes
        # remove trailing / from folder name and substitute a /10N*
        # or whatever tile we're working on

        if 'points' in iterate_by:
            points_folder = '{}{}*'.format(points_folder, ns_tile)
        if 'polygons' in iterate_by:
            polygons_folder = '{}*{}*'.format(polygons_folder, ns_tile)

    points_folder = points_folder.replace('s3://', 's3a://')
    points_fields = points_fields_dict[analysis_type]

    application_props = """
spark.app.name=YARN Points in World
output.path=hdfs:///user/hadoop/output
output.sep=,
points.y=1
points.path={0}
points.fields={1}
points.x=0
reduce.size=0.5
polygons.path={2}
polygons.wkt=0
polygons.fields=1,2,3,4,5,6,7,8
analysis.type={3}
    """.format(points_folder, points_fields, polygons_folder, analysis_type)

    with open("application.properties", 'w') as app_props:
        app_props.write(application_props)


def call_pip(dryrun):
    if not dryrun:
        subprocess.call(['hdfs', 'dfs', '-rm', '-r', 'output'])
        pip_cmd = ['spark-submit', '--master', 'yarn']
        pip_cmd += ['--executor-memory', '20g']
        pip_cmd += ['--jars', r'target/libs/jts-core-1.14.0.jar', 'target/spark-pip-0.3.jar']

        subprocess.check_call(pip_cmd)


def check_output_exists(args, ns_tile=None):

    analysis_type = args.analysis_type
    iterate_by = args.iterate_by
    output_folder = args.output_folder

    conn = S3Connection(host="s3.amazonaws.com")
    parsed = urlparse(output_folder)

    if iterate_by:
        out_csv = '{}.csv'.format(ns_tile)

    else:
        out_csv = '{}_all.csv'.format(analysis_type)

    # connect to the s3 bucket
    bucket = conn.get_bucket(parsed.netloc)

    # remove leading slash, for some reason
    prefix = parsed.path[1:]

    # loop through file names in the bucket
    full_path_list = [key.name for key in bucket.list(prefix=prefix)]

    # unpack the filename from the list of files
    filename_only_list = [x.split('/')[-1] for x in full_path_list]


    return out_csv in filename_only_list


def upload_to_s3(analysis_type, s3_output_folder, dryrun, ns_tile_name=None):
    if not dryrun:
        cmd = ['hdfs', 'dfs', '-ls', 'output/']
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        out, err = p.communicate()

        if "_SUCCESS" not in out:
            raise ValueError("process failed, success file not found")

        # Extent outputs should be extent/10N.csv, 20N.csv etc
        if analysis_type == 'extent' or analysis_type == 'biomass':
            csv_name = ns_tile_name + '.csv'
        else:
            csv_name = analysis_type + '.csv'

        cmd = ['hdfs', 'dfs', '-getmerge', 'output/', csv_name]
        subprocess.check_call(cmd)

        cmd = ['aws', 's3', 'mv', csv_name, s3_output_folder]
        subprocess.check_call(cmd)

        #copy application.properties file into the out_path
        cmd = ['aws', 's3', 'cp', 'application.properties', s3_output_folder]
        subprocess.check_call(cmd)
