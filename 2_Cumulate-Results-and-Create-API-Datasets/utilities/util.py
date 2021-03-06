import os
import subprocess
import uuid

import pandas as pd


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


def write_output(cumsum_df, input_file):

    print 'dumping records to local CSV'
    if os.path.isdir(input_file):
        output_file = os.path.join(input_file, 'output.csv')
    else:
        output_file = os.path.splitext(input_file)[0] + '_processed.csv'

    cumsum_df.to_csv(output_file, index=False)

    return output_file


def qc_loss_extent(df):

    if not df[(df.area_admin < 0) | (df.area_gain < 0) | (df.area_extent < 0) |
              (df.area_poly_aoi < 0) | (df.area_extent_2000 < 0) | (df.area_loss < 0) |
              (df.emissions < 0) | (df.year < 0) ].empty:
        raise ValueError('Final dataframe has negative values in area fields')


def check_for_duplicates(df, polynames_with_bound1):

    cols = df.columns.tolist()

    # see if we're dealing with loss (12 cols) or a different input df
    if len(cols) < 12:
        col_idx = -1
    else:
        col_idx = -2

    group_list = cols[0:col_idx]
    sum_list = cols[col_idx:]

    grouped = df.groupby(group_list)[sum_list].sum().reset_index()

    if df.shape != grouped.shape:
        df_size = df.groupby(group_list).size().reset_index()
        print df_size[df_size[0] > 1].head()
        raise ValueError('\n\n\nAppears to be some duplicate values in input df. ' \
                          'If data your processing has valid bound1 and bound2 entries ' \
                          'that make it unique, you may have to add it to the list in ' \
                          'utilities/util.py, on line 150. Currently that list is: ' \
                          '{}'.format(', '.join(polynames_with_bound1)))


def input_csvs_to_df(args):

    gain_headers = ['polyname', 'bound1', 'bound2', 'bound3', 'bound4', 'iso', 'adm1', 'adm2', 'area_gain']
    area_headers = ['polyname', 'bound1', 'bound2', 'bound3', 'bound4', 'iso', 'adm1', 'adm2', 'area_poly_aoi']

    if args.source_dir:
        source_dir = args.source_dir
        check_source_dir(source_dir)

        loss_df = read_df(os.path.join(source_dir, 'loss.csv'))
        extent_2000_df = read_df(os.path.join(source_dir, 'extent2000.csv'))
        extent_2010_df = read_df(os.path.join(source_dir, 'extent2010.csv'))
        gain_df = read_df(os.path.join(source_dir, 'gain.csv'), gain_headers)
        poly_aoi_df = read_df(os.path.join(source_dir, 'area.csv'), None, area_headers)

    else:
        loss_df = read_df(args.loss_dataset)
        extent_2000_df = read_df(args.extent_2000_dataset)
        extent_2010_df = read_df(args.extent_2010_dataset)
        gain_df = read_df(args.gain_dataset, gain_headers)
        poly_aoi_df = read_df(args.polygon_dataset)

    return loss_df, extent_2000_df, extent_2010_df, gain_df, poly_aoi_df


def check_source_dir(source_dir):

    file_list = os.listdir(source_dir)
    files_required = ['loss', 'gain', 'extent2000', 'extent2010', 'area']

    missing_files = [x for x in files_required if x + '.csv' not in file_list]

    if missing_files:
        raise ValueError('{}.csv required but not found in {}'.format(missing_files[0], source_dir))


def filter_out_bad_combos(poly, iso_list, df):

    # filter out polygon from any country except those in list
    poly_iso_codes = df[df.polyname.str.contains(poly)].iso.unique().tolist()
    invalid_iso_codes = [x for x in poly_iso_codes if x not in iso_list]

    # remove any row that has one of these iso codes and polyname
    df = df[~(df.polyname.str.contains(poly) & df.iso.isin(invalid_iso_codes))]

    return df


def read_df(csv_path, gain_headers=None, area_headers=None):

    df = pd.read_csv(csv_path, na_values=-9999, encoding='utf-8',
                         dtype={'polyname': str, 'bound1': str, 'bound2': str})

    if gain_headers:
        # drop the thresh column from gain
        df = df.drop(df.columns[8], axis=1)

        # assign column headers. This is missing because gain skips the post processing step that would do this
        df.columns = gain_headers

        # convert gain area from m2 to ha
        df['area_gain'] = df.area_gain / 10000

    if area_headers:
        df.columns = area_headers
    # set all values of bound1, 2, 3 and 4 to null unless plantatations, biomes, or tsc are involved
    polynames_with_bound1 = ['plantation', 'biome', 'tsc', 'riverbasin', 'ecoregion', 'ifl']
    polynames_with_bound1_str = '|'.join(polynames_with_bound1)
    df.loc[~df['polyname'].str.contains(polynames_with_bound1_str), ['bound1', 'bound2']] = None

    # and set bound3 and bound4 columns to -9999 - plantations is
    # our only dataset with attribute values
    df[['bound3', 'bound4']] = None

    # correct ifl_2013 --> ifl
    # a little tricky because we have many ifl_2013__* datasets
    df.polyname = df.polyname.str.replace('ifl_2013', 'ifl')

    # to allow for better joining
    df = df.fillna(-9999)

    # filter out primary forest from any country except IDN and COD
    primary_forest_iso_codes = df[df.polyname.str.contains('primary')].iso.unique().tolist()
    invalid_iso_codes = [x for x in primary_forest_iso_codes if x not in ['COD', 'IDN']]

    # remove any row that has one of these iso codes and polyname like primary
    df = df[~(df.polyname.str.contains('primary') & df.iso.isin(invalid_iso_codes))]

    # check for dupes - should be unique polyname | bound1 | bound2 | iso | adm1 | adm2 (and year + thresh if loss)
    print 'checking {} for dupes'.format(csv_path)
    check_for_duplicates(df, polynames_with_bound1)

    # remove invalid poly/iso combos
    whitelist = {'mining': ['CMR', 'KHM', 'CAN', 'COL', 'COG', 'GAB', 'COD', 'PER', 'BRA', 'MEX'],
                 'primary_forest': ['COD', 'IDN'],
                 'idn_mys_peatlands': ['IDN', 'MYS'],
                 'plantations': ['BRA', 'KHM', 'COL', 'IDN', 'LBR', 'MYS', 'PER'],
                 'managed_forest': ['CMR', 'CAN', 'CAR', 'COD', 'GNQ', 'GAB', 'IDN', 'LBR', 'COG'],
                 'idn_forest_moratorium': ['IDN'],
                 'wood_fiber': ['IDN', 'COG', 'MYS'],
                 'oil_palm': ['CMR', 'IDN', 'LBR', 'COG']}

    for poly, iso_list in whitelist.iteritems():
        df = filter_out_bad_combos(poly, iso_list, df)

    # remove XCA and TWN
    df = df[~df.iso.isin(['XCA', 'TWN'])]

    return df
