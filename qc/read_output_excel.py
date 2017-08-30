import pandas as pd

# read sheet
raw_df = pd.read_excel('tree_cover_stats_2016.xlsx', sheetname='Loss (2001-2016) by Country', skiprows=[0])

thresh_list = [10, 15, 20, 25, 30, 50, 75]

# remove totals columns
# https://stackoverflow.com/a/406408/4355916
raw_df = raw_df.filter(regex=r'^((?!TOTAL).)*$', axis=1)

# create the first dataframe for threshold 10,
# by only selecting columns with names = 4 digits or "Country"
df = raw_df.filter(regex=r'^[0-9]{4}$|Country', axis=1)
df['thresh'] = thresh_list[0]

print df
print df.columns

for thresh_id, thresh_val in zip(range(1, 7), thresh_list[1:]):
    # grab each individual thresh_df (all columns named .{thresh id}
    thresh_df = raw_df.filter(regex=r'\.{}|Country'.format(thresh_id), axis=1)
    
    # rename columns
    thresh_df.columns = ['Country'] + range(2001, 2017)
    
    # add thresh column
    thresh_df['thresh'] = thresh_val
    print thresh_df.columns
    
    df = df.append(thresh_df)
    
print df
    
    
    
    