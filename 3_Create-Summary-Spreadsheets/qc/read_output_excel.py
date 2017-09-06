import pandas as pd


def unstack_loss_sheet(excel_file, sheet_name):

    # read sheet
    raw_df = pd.read_excel(excel_file, sheetname=sheet_name, skiprows=[0])

    thresh_list = [10, 15, 20, 25, 30, 50, 75]

    # remove totals columns
    # https://stackoverflow.com/a/406408/4355916
    raw_df = raw_df.filter(regex=r'^((?!TOTAL).)*$', axis=1)

    # create the first dataframe for threshold 10,
    # by only selecting columns with names = 4 digits or "Country"
    df = raw_df.filter(regex=r'^[0-9]{4}$|Country', axis=1)
    df['thresh'] = thresh_list[0]

    for thresh_id, thresh_val in zip(range(1, 7), thresh_list[1:]):
        # grab each individual thresh_df (all columns named .{thresh id}
        thresh_df = raw_df.filter(regex=r'\.{}|Country'.format(thresh_id), axis=1)

        # rename columns
        thresh_df.columns = ['Country'] + range(2001, 2017)

        # add thresh column
        thresh_df['thresh'] = thresh_val
        print thresh_df.columns

        df = df.append(thresh_df)

    return df

if __name__ == '__main__':
    unstack_loss_sheet('tree_cover_stats_2016.xlsx', 'Loss (2001-2016) by Country')
