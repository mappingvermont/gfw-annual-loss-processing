import argparse

from utilities import util, cumsum


def main():

    # Parse commandline arguments
    parser = argparse.ArgumentParser(description='Cumsum hadoop output')
    parser.add_argument('--input', '-i', required=True, help='an input CSV or folder on S3 or the local file system')
    parser.add_argument('--analysis-name', '-a', required=True, help='choose loss, extent2000, extent2010, annualGain, cumulGain, grossEmis, or netEmis')
    parser.add_argument('--max-year', '-y', type=int, help='the max year of the loss data, if applicable')
    parser.add_argument('--biomass-thresh', '-b', help='10,20,30 etc. whatever thresh the biomass raster was created for')

    parser.add_argument('--no-emissions', dest='emissions', action='store_false',
                        help='used when processing extent, biomass, or gain data')
    parser.set_defaults(emissions=True)

    parser.add_argument('--no-years', dest='years', action='store_false',
                        help='used when processing extent, biomass, or gain data')
    parser.set_defaults(years=True)

    args = parser.parse_args()

    if args.years and not args.max_year:
        raise ValueError('Max year must be specified if the dataset has year values included')

    if args.years and args.max_year < 2000:
        raise ValueError('Max year must be >2000, e.g., 2017')

    analysis_names = ['loss', 'extent2000', 'extent2010', 'annualGain', 'cumulGain', 'grossEmis','netEmis']
    if args.analysis_name not in analysis_names:
        raise ValueError('Analysis name must be one of {}'.format(", ".join(analysis_names)))

    local_data = util.download_data(args.input)
    print local_data

    cumsum_df = cumsum.tabulate(local_data, args)

    output_csv = util.write_output(cumsum_df, local_data)

    print 'Cumsummed CSV is saved here {}'.format(output_csv)


if __name__ == "__main__":
    main()
