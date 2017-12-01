import os
import argparse
import pandas as pd
import json
import io


def main():

    # Parse commandline arguments
    parser = argparse.ArgumentParser(description='Clean up polynames, join loss and extent data')
    parser.add_argument('--loss-dataset', '-l', required=True, help='path to cum-summed loss csv')
    parser.add_argument('--extent-dataset', '-e', required=True, help='path to cum-summed extent csv')
    args = parser.parse_args()

    loss_df = read_df(args.loss_dataset)
    extent_df = read_df(args.extent_dataset)

    merged = join_loss_extent(loss_df, extent_df)
    
    print 'dumping to output json'
    as_dict = {'data': merged.to_dict(orient='records')}

    with io.open('final.json', 'w', encoding='utf-8') as thefile:
        thefile.write(json.dumps(as_dict, ensure_ascii=False))


def join_loss_extent(loss_df, extent_df):
    
    join_list = ['polyname', 'bound1', 'bound2', 'bound3', 'bound4', 
                 'iso', 'adm1', 'adm2', 'thresh']
    suffixes = ['_extent', '_loss']

    merged = pd.merge(extent_df, loss_df, how='left', on=join_list, suffixes=suffixes)
    
    # temporarily set nodata to -9999 - pandas can't group nd values
    merged = merged.fillna(-9999)
    merged['year'] = merged.year.astype(int)

    # add area_extent to join list for grouping
    join_list.extend(['area_extent', 'area_gadm28'])

    print 'grouping data by adm2 and polygon'
    # source: https://stackoverflow.com/questions/40470954/
    grouped = (merged.groupby(join_list, as_index=False)
                 .apply(lambda x: x[['year', 'area_loss', 'emissions']].to_dict('r'))
                 .reset_index()
                 .rename(columns={0: 'year_data'}))

    return grouped


def read_df(csv_path):
    
    df = pd.read_csv(csv_path, na_values=-9999, encoding='utf-8')
    
    # replace old suffix name with ''
    df['polyname'] = df['polyname'].apply(lambda x: x.replace('_int_diss_gadm28_large.tsv', ''))

    # clean up other names as well
    df['polyname'] = df['polyname'].apply(lambda x: x.replace('gfw_', ''))
    df['polyname'] = df['polyname'].apply(lambda x: x.replace('manged_forests', 'managed_forests'))
    df.loc[df['polyname'] == 'idn_mys_peat_lands', 'polyname'] = 'idn_mys_peatlands'
    
    # fix order of some filenames-- should be ifl_2013, plantations, or primary_forest
    df.loc[df['polyname'] == 'wdpa__ifl_2013', 'polyname'] = 'ifl_2013__wdpa'

    # can ignore ifl_2000 data-- not using it at present 
    # mistakenly include wdpa and plantations twice. 
    # drop this because it's in the wrong order (wdpa, then plantations)
    df = df[~df.polyname.isin(['ifl_2000', 'wdpa__plantations'])]

    # set all values of bound1, 2, 3 and 4 to null unless plantatations are involved
    df.loc[~df['polyname'].str.contains(r'plantation|biome'), ['bound1', 'bound2']] = None
    
    # and set bound3 and bound4 columns to -9999 - plantations is
    # our only dataset with attribute values
    df[['bound3', 'bound4']] = None

    return df


if __name__ == '__main__':
    main()

