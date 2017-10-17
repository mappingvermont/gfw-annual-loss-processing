import argparse

from utilities import util, api, cumsum


def main():

    # Parse commandline arguments
    parser = argparse.ArgumentParser(description='Process hadoop output and create/overwrite datasets in the GFW API')
    parser.add_argument('--input', '-i', required=True, help='an input CSV or folder on S3 or the local file system')
    parser.add_argument('--environment', '-e', help='version of the API', choices=('prod', 'staging'))

    parser.add_argument('--dataset-id', '-d', help='the ID of the dataset to overwrite')
    parser.add_argument('--tags', '-t', nargs='+', help='tags for the dataset, required if creating a new dataset')
    parser.add_argument('--name', '-n', help='dataset name, if creating a new dataset')

    parser.add_argument('--no-emissions', dest='emissions', action='store_false')
    parser.set_defaults(emissions=True)

    parser.add_argument('--no-years', dest='years', action='store_false')
    parser.set_defaults(years=True)

    parser.add_argument('--local', dest='local', action='store_true')
    parser.set_defaults(local=False)

    args = parser.parse_args()

    if args.dataset_id or (args.tags and args.name) or args.local:

        local_data = util.download_data(args.input)
        print local_data

        cumsum_df = cumsum.tabulate(local_data, args)

        s3_file = util.push_to_s3(cumsum_df, local_data, args.local)

        if args.local:
            print 'Cumsummed CSV is saved here {}'.format(s3_file)

        else:
            if args.dataset_id:
                api.overwrite(s3_file, args.environment, args.dataset_id)

            else:
                api.create(s3_file, args.environment, args.tags, args.name)

    else:
        raise ValueError('Either dataset-id, dataset name and tags, or --local required. '
                         'Must be creating/overwriting a dataset, or doing local processing')


if __name__ == "__main__":
    main()
