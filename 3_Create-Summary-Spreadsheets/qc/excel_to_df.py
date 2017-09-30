import pandas as pd


thresh_list = [10, 15, 20, 25, 30, 50, 75]

# read formatted loss table, unstack, and return
# in country | thresh | year1 | year2 | year3 . . . format
def loss(excel_file, sheet_name):

    # read sheet
    raw_df = pd.read_excel(excel_file, sheetname=sheet_name, skiprows=[0])

    # remove totals columns
    # https://stackoverflow.com/a/406408/4355916
    raw_df = raw_df.filter(regex=r'^((?!TOTAL).)*$', axis=1)

    # create the first dataframe for threshold 10,
    # by only selecting columns with names = 4 digits or "Country"
    df = raw_df.filter(regex=r'^[0-9]{4}$|Country', axis=1)

    # rename Country_Subnat1 column if it exists to simplify pivot process
    df = df.rename(columns={'Country_Subnat1': 'Country'})

    df['thresh'] = thresh_list[0]

    for thresh_id, thresh_val in zip(range(1, 7), thresh_list[1:]):
        # grab each individual thresh_df (all columns named .{thresh id})
        thresh_df = raw_df.filter(regex=r'\.{}|Country'.format(thresh_id), axis=1)

        # rename columns
        thresh_df.columns = ['Country'] + range(2001, 2017)

        # add thresh column
        thresh_df['thresh'] = thresh_val

        df = df.append(thresh_df)

    return df


def extent(excel_file, sheet_name):

    df = pd.read_excel(excel_file, sheetname=sheet_name, skiprows=[0])
    df.columns = ['Country'] + thresh_list

    return df

def gain(excel_file, sheet_name):

    df = pd.read_excel(excel_file, sheetname=sheet_name, skiprows=[0])
    df.columns = ['Country', 'area']

    return df

if __name__ == '__main__':
    unstack_loss_sheet('tree_cover_stats_2016.xlsx', 'Loss (2001-2016) by Country')
