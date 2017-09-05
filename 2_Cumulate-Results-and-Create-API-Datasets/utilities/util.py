import os
import subprocess
import json
import uuid


def download_data(input_dataset):

    file_ext = os.path.splitext(input_dataset)[1]

    if file_ext not in ['', '.csv']:
        raise ValueError('Unknown file extension {}, expected directory or CSV'.format(file_ext))

    if os.path.exists(input_dataset):
        return os.path.abspath(input_dataset)

    elif input_dataset[0:5] == r's3://':
        root_dir = os.path.dirname(os.path.dirname(__file__))
        processing_dir = os.path.join(root_dir, 'processing')

        guid = str(uuid.uuid4())

        if not os.path.exists(processing_dir):
            os.mkdir(processing_dir)

        # If the input is a single CSV
        if file_ext == '.csv':
            temp_dir = os.path.join(processing_dir, guid)
            os.mkdir(temp_dir)

            fname = os.path.basename(input_dataset)
            local_path = os.path.join(temp_dir, fname)

            cmd = ['aws', 's3', 'cp', input_dataset, local_path]

        # Or if it's a directory
        else:
            local_path = os.path.join(processing_dir, guid)

            cmd = ['aws', 's3', 'sync', input_dataset, local_path]

        subprocess.check_call(cmd)

        return local_path

    else:
        raise ValueError("Dataset {} does not exist locally and doesn't have an s3:// URL ".format(input_dataset))


def push_to_s3(cumsum_df, input_file, local_save):

    record_list = cumsum_df.to_dict(orient='records')

    print 'dumping records to local JSON'
    if os.path.isdir(input_file):
        output_file = os.path.join(input_file, 'output.json')
    else:
        output_file = os.path.splitext(input_file)[0] + '.json'

    fname = os.path.basename(output_file)

    record_dict = {'data': record_list}

    with open(output_file, 'wb') as the_file:
        json.dump(record_dict, the_file)

    if local_save:
        output_dir = os.path.dirname(output_file)
        csv_name = os.path.splitext(fname)[0] + '_processed.csv'
        csv_file = os.path.join(output_dir, csv_name)

        cumsum_df.to_csv(csv_file, index=False)
        output = csv_file

    else:

        print 'Copying output JSON to s3'
        s3_outfile = r's3://gfw2-data/alerts-tsv/output/to-api/{}'.format(fname)
        cmd = ['aws', 's3', 'cp', output_file, s3_outfile]

        subprocess.check_call(cmd)
        output = r'http://gfw2-data.s3.amazonaws.com/alerts-tsv/output/to-api/{}'.format(fname)

    return output


def load_json_from_token(file_name):

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    token_file = os.path.join(root_dir, 'tokens', file_name)

    with open(token_file) as data_file:
        data = json.load(data_file)

    return data
