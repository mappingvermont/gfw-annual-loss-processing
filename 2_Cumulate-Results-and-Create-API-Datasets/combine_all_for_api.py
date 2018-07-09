import os
import argparse
import pandas as pd
import json
import io

from utilities import util


def main():

    # Parse commandline arguments
    parser = argparse.ArgumentParser(description='Join loss, extent, gain and area data to API format')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--source-dir', '-s', help='path to an input directory with datasets all input CSVs')
    group.add_argument('--loss-dataset', '-l', help='path to cum-summed loss csv')
    parser.add_argument('--extent-2000-dataset', '-e2000', help='path to cum-summed extent 2000 csv')
    parser.add_argument('--extent-2010-dataset', '-e2010', help='path to cum-summed extent 2010 csv')
    parser.add_argument('--gain-dataset', '-g', help='path to gain dataset')
    parser.add_argument('--polygon-dataset', '-p', help='path to CSV of polygon AOI areas, grouped by polyname/bound1/bound2/bound3/bound4/iso/adm1/adm2')

    args = parser.parse_args()

    loss_df, extent_2000_df, extent_2010_df, gain_df, poly_aoi_df = util.input_csvs_to_df(args)

    adm2_area_df = poly_aoi_df_to_adm2_area(poly_aoi_df)

    extent_with_area = add_area_to_extent_df(extent_2000_df, extent_2010_df, adm2_area_df, poly_aoi_df, gain_df)

    field_list = ['polyname', 'bound1', 'bound2', 'bound3', 'bound4', 'thresh']

    merged = join_loss_extent(loss_df, extent_with_area, field_list)

    util.qc_loss_extent(merged)

    for adm_level in range(0, 3):
        write_output(merged, adm_level, field_list)


def poly_aoi_df_to_adm2_area(poly_aoi_df):

    # take the poly_aoi_df and filter it to get only admin data
    adm2_area_df = poly_aoi_df.copy()
    adm2_area_df = adm2_area_df[adm2_area_df.polyname == 'admin']

    # remove polyname and boundary fields from poly_aoi df
    adm2_area_df = adm2_area_df.drop(adm2_area_df.columns[[0, 1, 2, 3, 4]], axis=1)

    adm2_area_df = adm2_area_df.rename(columns={'area_poly_aoi': 'area_admin'})

    return adm2_area_df


def write_output(df, adm_level, field_list):

    # build field list for grouping
    adm_list = ['iso', 'adm1', 'adm2'][0: adm_level + 1]

    area_fields = ['area_extent', 'area_admin', 'area_poly_aoi', 'area_gain', 'area_extent_2000', 'area_loss', 'emissions']
    group_list = field_list + adm_list

    print 'grouping data by adm level {} and polygon'.format(adm_level)
    # source: https://stackoverflow.com/questions/40470954/
    # first group and sum to adm0/adm1/adm2 level as appropriate
    # then group again to collapse year/loss/emissions into nested JSON
    grouped = (df.groupby(group_list + ['year'])[area_fields]
                 .sum()
                 .reset_index()
                 .groupby(group_list + area_fields[0:5], as_index=False)
                 .apply(lambda x: x[['year', 'area_loss', 'emissions']].to_dict('r'))
                 .reset_index()
                 .rename(columns={0: 'year_data'}))

    # sort so that rows with a valid bound2 values are on top
    # necessary so that reads column data type properly
    grouped = grouped.sort_values('bound2', ascending=False)

    print 'dumping to output json'
    as_dict = {'data': grouped.to_dict(orient='records')}

    with io.open('adm{}.json'.format(adm_level), 'w', encoding='utf-8') as thefile:
        thefile.write(json.dumps(as_dict, ensure_ascii=False))


def add_area_to_extent_df(extent_2000_df, extent_2010_df, adm2_area_df, poly_area_df, gain_df):

    # join extent2000 to admin areas for each polygon
    # doesn't include anything to do with thresh - one to many
    join_field_list = ['iso', 'adm1', 'adm2']
    first_merge = pd.merge(extent_2000_df, adm2_area_df, how='left', on=join_field_list)

    # now join to the areas of our AOIs - specific to polyname + bounds
    join_field_list.extend(['polyname', 'bound1', 'bound2', 'bound3', 'bound4'])
    second_merge = pd.merge(first_merge, poly_area_df, how='left', on=join_field_list)

    # admin polygons don't have a default area_poly_aoi, so set it = gadm area
    second_merge.loc[second_merge.area_poly_aoi.isnull(), 'area_poly_aoi'] = second_merge['area_admin']

    # gain data doesn't have thresh, but has unique values per poly AOI as well
    third_merge = pd.merge(second_merge, gain_df, how='left', on=join_field_list)
    third_merge.loc[third_merge.area_gain.isnull(), 'area_gain'] = 0

    # now join to 2010 extent - add thresh to join list
    join_field_list.extend(['thresh'])
    fourth_merge = pd.merge(third_merge, extent_2010_df, how='left', on=join_field_list)

    return fourth_merge


def join_loss_extent(loss_df, extent_df, field_list):

    join_list = field_list + ['iso', 'adm1', 'adm2']
    suffixes = ['_extent', '_loss']

    merged = pd.merge(extent_df, loss_df, how='left', on=join_list, suffixes=suffixes)

    # temporarily set nodata to -9999 - pandas can't group nd values
    bound_list = [x for x in merged.columns if 'bound' in x]
    merged[bound_list] = merged[bound_list].fillna(-9999)

    # for area values, set values to 0 where we have nodata
    area_list = [x for x in merged.columns if 'area' in x]
    merged[area_list] = merged[area_list].fillna(0)

    # also update year to be -9999 if no loss data for this year
    # we'll deal with these -9999 values for year later on
    merged['year'] = merged.year.fillna(-9999)

    # explicitly set proper fieldnames to int
    for field_name in ['year', 'adm1', 'adm2']:
        merged[field_name] = merged[field_name].astype(int)

    for field_name in bound_list:
        merged[field_name] = merged[field_name].astype(unicode)

    # there are many adm2 areas with extent data but without loss
    # these are currently rows in the dataset with a single loss year
    # of -9999. Need to join to these, and a create a record
    # for each year with 0 loss and 0 emissions
    max_year = loss_df.year.max()
    dummy_df = pd.DataFrame(range(2001, max_year + 1), columns=['dummy_year'])
    dummy_df['year'] = -9999

    # now join this dummy dataframe to merged
    merged = pd.merge(merged, dummy_df, how='left', on='year')

    # then update anything where year = -9999 to be the dummy_year value
    # and set loss and emissions for these instances to 0
    merged.loc[merged.year == -9999, ['area_loss', 'emissions']] = 0
    merged.loc[merged.year == -9999, ['year']] = merged['dummy_year']

    del merged['dummy_year']

    return merged


if __name__ == '__main__':
    main()
