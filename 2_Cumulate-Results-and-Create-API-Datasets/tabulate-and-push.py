import argparse

from utilities import util, cumsum


def main():

    # Parse commandline arguments
    parser = argparse.ArgumentParser(description='Process hadoop output and create/overwrite datasets in the GFW API')
    parser.add_argument('--input', '-i', required=True, help='an input CSV or folder on S3 or the local file system')
    parser.add_argument('--max-year', '-y', type=int, help='the max year of the loss data, if applicable')
    parser.add_argument('--biomass-thresh', '-b', help='10,20,30 etc. whatever thresh the biomass raster was created for')

    parser.add_argument('--no-emissions', dest='emissions', action='store_false')
    parser.set_defaults(emissions=True)

    parser.add_argument('--no-years', dest='years', action='store_false')
    parser.set_defaults(years=True)

    args = parser.parse_args()

    if args.years and not args.max_year:
        raise ValueError('Max year must be specified if the dataset has year values included')

    local_data = util.download_data(args.input)
    print local_data

    cumsum_df = cumsum.tabulate(local_data, args)

    s3_file = util.push_to_s3(cumsum_df, local_data)
    
    print 'Cumsummed CSV is saved here {}'.format(s3_file)


if __name__ == "__main__":
    main()
