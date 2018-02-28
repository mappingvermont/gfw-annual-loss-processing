import argparse
import pandas as pd

import excel_to_df

thresh_list = [10, 15, 20, 25, 30, 50, 75]

def main():

    parser = argparse.ArgumentParser(description='QC Hansen Stats Spreadsheet')
    parser.add_argument('--excel_file', '-e', required=True)

    args = parser.parse_args()
    xl_src = args.excel_file

    # load dataframes
    extent2000_iso = excel_to_df.extent(xl_src, 'Extent (2000) by Country')
    extent2010_iso = excel_to_df.extent(xl_src, 'Extent (2010) by Country')

    extent2000_subnat = excel_to_df.extent(xl_src, 'Extent (2000) by Subnat1')
    extent2010_subnat = excel_to_df.extent(xl_src, 'Extent (2010) by Subnat1')

    loss_iso = excel_to_df.loss(xl_src, 'Loss (2001-2016) by Country')
    loss_subnat = excel_to_df.loss(xl_src, 'Loss (2001-2016) by Subnat1')

    gain_iso = excel_to_df.gain(xl_src, 'Gain (2001-2012) by Country')
    gain_subnat = excel_to_df.gain(xl_src, 'Gain (2001-2012) by Subnat1')

    # check that the area value declines as the threshold increases
    # for each country or country_admin row
    check_extent_thresh(extent2000_iso, 'extent iso')
    check_extent_thresh(extent2010_iso, 'extent iso')

    check_extent_thresh(extent2000_subnat, 'extent subnat')
    check_extent_thresh(extent2010_subnat, 'extent subnat')

    # check that the area values decline as the threshold increases
    # for each year of loss in each country or country_admin row
    check_loss_iso_thresh(loss_iso, 'loss iso')
    check_loss_iso_thresh(loss_subnat, 'loss subnat')

    # check that the sum of loss from 2001 - 2014 is always less
    # than the forest extent area for that country or country_admin
    # boundary, by threshold
    compare_loss_and_extent(loss_iso, extent2000_iso, 'country')
    compare_loss_and_extent(loss_subnat, extent2000_subnat, 'subnat')

    # ensure that subnat totals always add up to nat values for gain, loss and extent
    compare_nat_to_subnat(extent2000_iso, extent2000_subnat, 'extent')
    compare_nat_to_subnat(extent2010_iso, extent2010_subnat, 'extent')
    compare_nat_to_subnat(loss_iso, loss_subnat, 'loss')
    compare_nat_to_subnat(gain_iso, gain_subnat, 'gain')


def compare_nat_to_subnat(nat_src, subnat_src, df_type):

    # copy dataframes so that we don't edit the originals
    nat_df = nat_src.copy()
    subnat_df = subnat_src.copy()

    # create an ID column for just country name
    subnat_df['id'] = subnat_df.Country.str.split('_').apply(pd.Series)[0]
    del subnat_df['Country']

    if df_type == 'loss':

        melted_nat = pd.melt(nat_df, id_vars=['Country', 'thresh'], value_name='area', var_name='year')

        melted_subnat = pd.melt(subnat_df, id_vars=['id', 'thresh'], value_name='area', var_name='year')

        melted_subnat = melted_subnat.groupby(['id', 'year', 'thresh'])['area'].sum().reset_index()
        melted_subnat.columns = ['Country', 'year', 'thresh', 'area']

        joined = pd.merge(melted_nat, melted_subnat, on=['Country', 'year', 'thresh'], suffixes=['_nat', '_subnat'])

    elif df_type == 'extent':
        melted_nat = pd.melt(nat_df, id_vars=['Country'], value_name='area', var_name='thresh')
        melted_subnat = pd.melt(subnat_df, id_vars=['id'], value_name='area', var_name='thresh')

        melted_subnat.columns = ['Country', 'thresh', 'area']
        melted_subnat = melted_subnat.groupby(['Country', 'thresh'])['area'].sum().reset_index()

        joined = pd.merge(melted_nat, melted_subnat, on=['Country', 'thresh'], suffixes=['_nat', '_subnat'])

    else:
        subnat_df = subnat_df.groupby(['id'])['area'].sum().reset_index()
        subnat_df.columns = ['Country', 'area']

        joined = pd.merge(nat_df, subnat_df, on=['Country'], suffixes=['_nat', '_subnat'])

    min_diff = min(joined.area_nat - joined.area_subnat)

    print 'min diff between nats and subnats for {} is {}'.format(df_type, min_diff)

    if min_diff < -0.0001:
        raise ValueError('Subnats should add to nats')


def compare_loss_and_extent(loss_df, extent_df, name):

    melted_loss = pd.melt(loss_df, id_vars=['Country', 'thresh'], value_name='area', var_name='year')

    sum_loss_by_thresh = melted_loss.groupby(['Country', 'thresh'])['area'].sum().reset_index()

    melted_extent = pd.melt(extent_df, id_vars=['Country'], value_name='area', var_name='thresh')

    joined = pd.merge(sum_loss_by_thresh, melted_extent, on=['Country', 'thresh'], suffixes=['_loss', '_extent'])

    min_diff = min(joined.area_extent - joined.area_loss)

    print 'min diff for extent to loss in {} is: {}'.format(name, min_diff)

    if min_diff < -0.000001:
        raise ValueError('Extent should never be less than loss')


def check_loss_iso_thresh(df, table_name):

    melted = pd.melt(df, id_vars=['Country', 'thresh'], value_name='area', var_name='year')

    for year in range(melted.year.min(), melted.year.max() + 1):
        year_df = melted[melted.year == year]

        thresh_pivot = year_df.pivot(index='Country', columns='thresh', values='area')

        year_table_name = table_name + ' ' + str(year)
        check_extent_thresh(thresh_pivot, year_table_name)


def check_extent_thresh(df, table_name):

    print 'Checking {}'.format(table_name)

    for i in range(0, len(thresh_list) - 1):
        low_thresh = thresh_list[i]
        high_thresh = thresh_list[i + 1]

        thresh_diff = min(df[low_thresh] - df[high_thresh])

        print 'Min diff for thresh {} - {} = {}'.format(low_thresh, high_thresh, thresh_diff)

        # originally compared to 0, but float weirdness
        # gives us numbers like these: -1.13686837722e-13
        if thresh_diff < -0.000001:
            raise ValueError('Thresh diff < 0')


if __name__ == '__main__':
    main()
