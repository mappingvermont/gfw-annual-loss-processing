import argparse

from utilities import util, api, cumsum


def main():

    # Parse commandline arguments
    parser = argparse.ArgumentParser(description='Process hadoop output and create/overwrite datasets in the GFW API')
    parser.add_argument('--input', '-i', required=True, help='an input CSV or folder on S3 or the local file system')
    parser.add_argument('--environment', '-e', required=True, help='version of the API', choices=('prod', 'staging'))
    parser.add_argument('--dataset-id', '-d', help='the ID of the dataset to overwrite')
    parser.add_argument('--tags', '-t', nargs='+', help='tags for the dataset, required if creating a new dataset')
    parser.add_argument('--boundary-fields', '-b', nargs='+', help='boundary field names if not iso/adm1/adm2')
    parser.add_argument('--custom-data-type', '-c', choices=('extent',), help='')

    args = parser.parse_args()

    if args.dataset_id or args.tags:
        local_data = util.download_data(args.input)
        print local_data

        cumsum_records = cumsum.tabulate(local_data, args.boundary_fields)

        s3_file = util.push_to_s3(cumsum_records, local_data)

        print s3_file

        # if args.dataset_id:
        #     api.overwrite(s3_file, args.environment, args.dataset_id)
        #
        # else:
        #     api.create(s3_file, args.environment, args.tags)

    else:
        raise ValueError('Either dataset-id or tags required-- must be creating or overwriting a dataset')


if __name__ == "__main__":
    main()
