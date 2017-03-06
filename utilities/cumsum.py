import os
import pandas as pd


def tabulate(input_data, custom_fields):

    df, boundary_fields = source_to_df(input_data, custom_fields)

    df = df.apply(lambda x: pd.to_numeric(x, errors='ignore'))

    # years in this CSV are stored as 1 - 14, should be 2001 - 2014
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
        for dummy_year in range(2001, 2016):
            all_combo_df['thresh'] = dummy_thresh
            all_combo_df['year'] = dummy_year
            dummy_df = dummy_df.append(all_combo_df)

    # outer join our dummy_df to df, so that we get proper
    # Nan values where we don't have data
    print 'joining to dummy data'
    join_fields = boundary_fields + ['thresh', 'year']
    joined_df = pd.merge(dummy_df, df, how='left', on=join_fields)

    # update all Nan values to be 0, so that they will be included
    # in the sum when we cumsum
    joined_df['area_raw'].fillna(0, inplace=True)
    # joined_df['emissions_raw'].fillna(0, inplace=True)

    print 'grouping by boundary fields, year and thresh'
    # grouped_df = joined_df.groupby(join_fields)['area_raw', 'emissions_raw'].sum().reset_index()
    grouped_df = joined_df.groupby(join_fields)['area_raw'].sum().reset_index()

    print 'Tabluating cum sum for thresh'
    # First sort the DF by threshold DESC, then cumsum, grouping by iso and year
    grouped_df = grouped_df.sort_values('thresh', ascending=False)

    cumsum_fields = boundary_fields + ['year']
    grouped_df['area'] = grouped_df.groupby(cumsum_fields)['area_raw'].cumsum()
    # grouped_df['emissions'] = grouped_df.groupby(cumsum_fields)['emissions_raw'].cumsum()

    # Delete the area_raw column-- this shouldn't go in the database
    del grouped_df['area_raw']
    # del grouped_df['emissions_raw']

    return grouped_df.to_dict(orient='records')


def source_to_df(input_data, boundary_fields):
    # base_fields = ['year', 'thresh', 'area_raw', 'emissions_raw']
    base_fields = ['year', 'thresh', 'area_raw']

    if not boundary_fields:
        boundary_fields = ['iso', 'adm1', 'adm2']

    print 'Reading df'
    if os.path.isdir(input_data):
        df = folder_to_df(input_data)
    else:
        df = pd.read_csv(input_data, header=None)

    num_cols = len(df.columns)
    expected_num = len(boundary_fields) + len(base_fields)

    if num_cols != expected_num:
        raise ValueError('Expected {} columns based on the input, found {} instead'.format(expected_num, num_cols))
    else:
        df.columns = boundary_fields + base_fields

    return df, boundary_fields


def folder_to_df(folder_path):

    csv_list = [os.path.join(folder_path, x) for x in os.listdir(folder_path)]
    df_list = [pd.read_csv(csv, header=None) for csv in csv_list]

    df = pd.concat(df_list)

    return df
