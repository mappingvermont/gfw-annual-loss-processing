import shutil
import os
import subprocess
from boto.s3.connection import S3Connection

import calc_tsv_extent


conn = S3Connection(host="s3.amazonaws.com")
bucket = conn.get_bucket('gfw2-data')


def download_jar():

    # check first to see if the target folder is already there:
    if not os.path.exists('target'):

        jar_file = 's3://gfw2-data/alerts-tsv/batch-processing/target_0.3.3.4.zip'

        cmd = ['aws', 's3', 'cp', jar_file, '.']
        subprocess.check_call(cmd)

        jar_name = os.path.basename(jar_file)
        cmd = ['unzip', jar_name]
        subprocess.check_call(cmd)


def write_props(analysis_type, fields_dict, ns_tile=None, extent_folder=None):
    
    # for our purposes, extent is the same as gain
    # four input fields (x, y, value and area)
    # and this is easier than editing the scala code to include a gain type
    if analysis_type == 'gain':
        analysis_type = 'extent'
                          
    path = fields_dict['points_path'][analysis_type].format(extent_folder, ns_tile)
    points_path = 's3a://gfw2-data/alerts-tsv/{}'.format(path)
    
    points_fields = fields_dict['points_fields'][analysis_type]

    application_props = """
spark.app.name=YARN Points in World
output.path=hdfs:///user/hadoop/output
output.sep=,
points.y=1
points.path={0}
points.fields={1}
points.x=0
reduce.size=0.5
polygons.path=s3a://gfw2-data/alerts-tsv/tsv-boundaries-climate/
polygons.wkt=0
polygons.fields=1,2,3,4,5,6,7,8
analysis.type={2}
    """.format(points_path, points_fields, analysis_type)

    with open("application.properties", 'w') as app_props:
        app_props.write(application_props)


def call_pip():
    subprocess.call(['hdfs', 'dfs', '-rm', '-r', 'output'])
    pip_cmd = ['spark-submit', '--master', 'yarn']
    pip_cmd += ['--executor-memory', '20g']
    pip_cmd += ['--jars', r'target/libs/jts-core-1.14.0.jar', 'target/spark-pip-0.3.jar']

    subprocess.check_call(pip_cmd)


def check_output_exists(analysis_type, ns_tile=None):

    if analysis_type == 'extent' or analysis_type == 'biomass':
    
        out_csv = ns_tile
        prefix = '{}/{}'.format(analysis_type, ns_tile)
        
    else:
        out_csv = '{}_all'.format(analysis_type)
        prefix = r'{}/{}'.format(analysis_type, out_csv)

    full_path_list = [key.name for key in bucket.list(prefix='alerts-tsv/output2016/{}'.format(prefix))]
    filename_only_list = [x.split('/')[-1] for x in full_path_list]

    return out_csv in filename_only_list


def upload_to_s3(analysis_type, s3_output_folder, ns_tile_name=None):
    cmd = ['hdfs', 'dfs', '-ls', 'output/']
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = p.communicate()

    if "_SUCCESS" not in out:
        raise ValueError("process failed, success file not found")

    # Extent outputs should be extent/10N.csv, 20N.csv etc
    if analysis_type == 'extent' or analysis_type == 'biomass':
        csv_name = ns_tile_name + '.csv'
        out_path = '{}/{}/'.format(s3_output_folder, analysis_type)
    else:
        csv_name = analysis_type + '.csv'
        out_path = r'{}/{}/'.format(s3_output_folder, analysis_type)

    cmd = ['hdfs', 'dfs', '-getmerge', 'output/', csv_name]
    print cmd
    subprocess.check_call(cmd)

    tsv_outputs = 's3://gfw2-data/alerts-tsv/{}'.format(out_path)
    cmd = ['aws', 's3', 'mv', csv_name, tsv_outputs]
    print cmd
    subprocess.check_call(cmd)
