import os
import argparse
import pandas as pd


def main():
    # Parse commandline arguments
    parser = argparse.ArgumentParser(description='Clean up polynames, join loss and extent data')
    parser.add_argument('--loss-dataset', '-l', required=True, help='path to cum-summed loss csv')
    parser.add_argument('--extent-dataset', '-e', required=True, help='path to cum-summed extent csv')
    args = parser.parse_args()

    loss_df = read_df(args.loss_dataset)
    extent_df = read_df(args.extent_dataset)

    loss_df.to_csv('loss.csv', index=None)
    extent_df.to_csv('extent.csv', index=None)


def read_df(csv_path):
    
    df = pd.read_csv(csv_path, na_values=-9999)

    # replace old suffix name with ''
    df['polyname'] = df['polyname'].apply(lambda x: x.replace('_int_diss_gadm28_large.tsv', ''))

    # clean up other names as well
    df['polyname'] = df['polyname'].apply(lambda x: x.replace('gfw_', ''))
    df['polyname'] = df['polyname'].apply(lambda x: x.replace('manged_forests', 'managed_forests'))
    df.loc[df['polyname'] == 'idn_mys_peat_lands', 'polyname'] = 'idn_mys_peatlands'
    
    # fix order of some filenames-- should be ifl_2013, plantations, or primary_forest
    # and then the polygon that they are intersected with
    df.loc[df['polyname'] == 'wdpa__plantations', 'polyname'] = 'plantations__wdpa'
    df.loc[df['polyname'] == 'wdpa__ifl_2013', 'polyname'] = 'ifl_2013__wdpa'

    # can ignore ifl_2000 data-- not using it at present 
    df = df[df.polyname != 'ifl_2000']

    # overwrite the values for boundary_field1 and boundary_field2 (-9999) for wdpa and plantations
    # plantations has real values for it's boundary fields, and they should be in these positions
    for i in range(1, 3):
        df.loc[df['polyname'] == 'plantations__wdpa', 'bound{}'.format(i)] = df['bound{}'.format(i + 2)]

    # set all values of bound1, 2, 3 and 4 to null unless plantatations are involved
    df.loc[~df['polyname'].str.contains(r'plantation|biome'), ['bound1', 'bound2']] = None
    
    # and set bound3 and bound4 columns to -9999 - plantations is
    # our only dataset with attribute values
    df[['bound3', 'bound4']] = None

    return df

    loss_df.to_csv('loss.csv', index=None)
    extent_df.to_csv('extent.csv', index=None)


if __name__ == '__main__':
    main()
