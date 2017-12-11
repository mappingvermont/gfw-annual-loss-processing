import os
import pandas as pd


def tabulate(input_data, args):

    if args.biomass_thresh:
        # no need to cumsum since we are running biomass extent one thresh at a time.
        df = read_df(input_data)
        
        # add in a thresh column
        df.insert(8, 'thresh', args.biomass_thresh)

        boundary_fields = ['polyname', 'bound1', 'bound2', 'bound3', 'bound4', 'iso', 'adm1', 'adm2', 'thresh', 'bio_per_pixel']
        df.columns = boundary_fields
        
        return df
        
    else:
        df, boundary_fields = source_to_df(input_data, args)

        # years in this CSV are stored as 1 - 14, should be 2001 - 2014
        if args.years:
            df['year'] = df['year'] + 2000

        # convert area in m2 to area in ha
        df['area_raw'] = df['area_raw'] / 10000

        # ID full universe of ISO/ADM1/ADM2 (or custom) combinations
        all_combo_df = df.groupby(boundary_fields).size().reset_index()

        # delete the count column that was added
        del all_combo_df[0]

        # create dummy df and store all possible combinations
        dummy_df = pd.DataFrame()

        # create a record for every combination of threshold
        # and year, in addition to the above iso/adm1/adm2
        for dummy_thresh in [0, 10, 15, 20, 25, 30, 50, 75]:
            all_combo_df['thresh'] = dummy_thresh

            if args.years:
                for dummy_year in range(2001, 2017):
                    all_combo_df['year'] = dummy_year
                    dummy_df = dummy_df.append(all_combo_df)

            else:
                dummy_df = dummy_df.append(all_combo_df)

        # outer join our dummy_df to df, so that we get proper
        # Nan values where we don't have data
        print 'joining to dummy data'
        join_fields = boundary_fields + ['thresh']
        if args.years:
            join_fields += ['year']

        joined_df = pd.merge(dummy_df, df, how='left', on=join_fields)

        # update all Nan values to be 0, so that they will be included
        # in the sum when we cumsum
        joined_df['area_raw'].fillna(0, inplace=True)

        if args.emissions:
            joined_df['emissions_raw'].fillna(0, inplace=True)

        print 'grouping by boundary fields, year and thresh'

        if args.emissions:
            grouped_df = joined_df.groupby(join_fields)['area_raw', 'emissions_raw'].sum().reset_index()
        else:
            grouped_df = joined_df.groupby(join_fields)['area_raw'].sum().reset_index()

        print 'Tabluating cum sum for thresh'
        # First sort the DF by threshold DESC, then cumsum, grouping by iso and year
        grouped_df = grouped_df.sort_values('thresh', ascending=False)

        if args.years:
            cumsum_fields = boundary_fields + ['year']
        else:
            cumsum_fields = boundary_fields

        grouped_df['area'] = grouped_df.groupby(cumsum_fields)['area_raw'].cumsum()

        # Delete the area_raw column-- this shouldn't go in the database
        del grouped_df['area_raw']

        if args.emissions:
            grouped_df['biomass'] = grouped_df.groupby(cumsum_fields)['emissions_raw'].cumsum()
            del grouped_df['emissions_raw']

        return grouped_df

def read_df(input_data):
    print 'Reading df'
    if os.path.isdir(input_data):
        df = folder_to_df(input_data)
    else:
        df = pd.read_csv(input_data, header=None)
        
    return df
    
    
def source_to_df(input_data, args):

    base_fields = ['thresh']

    if args.years:
        base_fields += ['year']

    base_fields += ['area_raw']

    if args.emissions:
        base_fields += ['emissions_raw']

    boundary_fields = ['polyname', 'bound1', 'bound2', 'bound3', 'bound4', 'iso', 'adm1', 'adm2']

    print 'Reading df'
    df = read_df(input_data)

    num_cols = len(df.columns)
    expected_num = len(boundary_fields) + len(base_fields)

    col_list = boundary_fields + base_fields
    df.columns = col_list
    
    # replace Nan values with -9999 so the groupby works
    for field in ['bound1', 'bound2', 'bound3', 'bound4']:
        df[field].fillna(-9999, inplace=True)
        
    df = df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
    
    # catch issue where thresh and year columns are in the wrong order
    if args.years:
        if df.year.max() >= 20:
    
            # switch positions of thresh and year
            # https://stackoverflow.com/a/2493980/4355916
            a, b = col_list.index('thresh'), col_list.index('year')
            col_list[b], col_list[a] = col_list[a], col_list[b]
        
            df.columns = col_list

    return df, boundary_fields


def folder_to_df(folder_path):
    print folder_path
    csv_list = [os.path.join(folder_path, x) for x in os.listdir(folder_path) if os.stat(os.path.join(folder_path, x)).st_size > 0]
    df_list = [pd.read_csv(csv, header=None) for csv in csv_list]

    df = pd.concat(df_list)

    return df
