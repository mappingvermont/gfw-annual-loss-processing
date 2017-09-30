import os
import subprocess
import json
from boto.s3.connection import S3Connection


conn = S3Connection(host="s3.amazonaws.com")
bucket = conn.get_bucket('gfw2-data')


def check_output_exists(s3_path):

    # find where alerts-tsv is in the string, then subset it from there
    # so we can get the prefix to the bucket name
    s3_index = s3_path.find('alerts-tsv')
    s3_prefix = s3_path[s3_index:]

    # if the list has any items, then the output data exists
    # the length is variable given that loss + gain only have one output file
    # but extent will have many
    return len([key.name for key in bucket.list(prefix=s3_prefix)]) > 0


def main():

    with open('config.json') as thefile:
        config = json.load(thefile)

    for item in config:

        raw_name = item['name']
        raw_src = item['src']
        raw_tags = item.get('tags', raw_name.lower().split()) + ['gadm28']
        raw_boundaries = item.get('fields')

        for analysis_type in ['gain', 'loss', 'extent']:

            name = raw_name + ' - {}'.format(analysis_type.title())
            tags = raw_tags + [analysis_type]

            cmd = ['python', 'tabulate-and-push.py', '-e', 'staging']
            cmd += ['--tags', ' '.join(tags), '--name', name]

            base_path = r's3://gfw2-data/alerts-tsv/output2016/{}/'.format(analysis_type)

            # extent uses a folder, not a csv
            if analysis_type == 'extent':
                src = base_path + r'{}/'.format(os.path.splitext(raw_src)[0])

            else:
                src = base_path + raw_src

            cmd += ['--input', src]

            # check if source exists
            if check_output_exists(src):

                if analysis_type in ['gain', 'extent']:
                    cmd += ['--no-emissions', '--no-years']

                # check if we have custom boundaries
                if raw_boundaries:
                    cmd += ['--boundary-fields', ' '.join(raw_boundaries)]

                try:
                    subprocess.check_call(cmd)
                except:
                    print 'error for dataset {} and type {}'.format(name, analysis_type)

            else:
                print 'output not found for src {}'.format(src)


if __name__ == '__main__':
    main()
