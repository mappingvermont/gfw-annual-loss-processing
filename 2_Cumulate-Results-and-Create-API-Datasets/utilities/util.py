import os
import subprocess
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


def push_to_s3(cumsum_df, input_file):

    print 'dumping records to local CSV'
    if os.path.isdir(input_file):
        output_file = os.path.join(input_file, 'output.csv')
    else:
        output_file = os.path.splitext(input_file)[0] + '_processed.csv'

    cumsum_df.to_csv(output_file, index=False)

    return output_file
