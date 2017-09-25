import sys
import cartoframes
import pandas as pd
import argparse
import os
import json
'''
import matplotlib
import matplotlib.pyplot as plt
matplotlib.style.use('ggplot')
'''
def load_json_from_token(file_name):
    root_dir = os.path.dirname(os.path.abspath(__file__))
    token_file = os.path.join(root_dir, 'tokens', file_name)
    
    with open(token_file) as data_file:
        data = json.load(data_file)
        
    return data

def get_api_tokens():
    api_key = load_json_from_token('creds.json')['api_key']

    return api_key
  
def read_new_loss(loss_processed, level):

    new_loss_df = pd.read_csv(loss_processed)

    # group new loss data by ISO to get sum of loss per year/thresh/country
    groupbyfields = ['iso', 'thresh', 'year']
    if level == 'subnat':
        groupbyfields += ['adm1']

    grouped_new_loss_df = new_loss_df.groupby(groupbyfields)['area'].sum().reset_index()
    if level == 'subnat':
        grouped_new_loss_df = grouped_new_loss_df.rename(columns={"adm1": "id1"})
    return grouped_new_loss_df

def create_2016(nat_new_loss, nat_old_loss, level):

    # return just the extent, gain, land numbers
    groupbyfields = ['iso', 'thresh', 'extent_2000', 'extent_perc', 'gain_perc', 'gain', 'land', 'country']
    if level == 'subnat':
        groupbyfields += ['id1']
    old_data_extent = nat_old_loss.groupby(groupbyfields).size().reset_index()
    old_data_extent = old_data_extent.drop(0, axis=1)
    
    # join old extent data (years aren't included) to new 2016 data
    joinfields = ['iso', 'thresh']
    if level == 'subnat':
        joinfields += ['id1']
    join_extent = pd.merge(old_data_extent, nat_new_loss, how='left', on=joinfields)
    
    #rename columns
    join_extent = join_extent.rename(columns={"area": "loss"})

    # filter only 2016 data
    join_extent = join_extent[join_extent.year == 2016]
    
    # calculate loss perc
    join_extent['loss_perc'] = (join_extent['loss']/join_extent['extent_2000'])*100


    join_extent.to_csv("new2016_{}.csv".format(level), encoding='utf-8')
    #df.to_csv(file_name, sep='\t', encoding='utf-8')

    return join_extent

def plot_df():

    plot_df = merged.groupby(['year', 'thresh']).sum()['loss_dif'].reset_index()

    plot_df.to_csv('out.csv', index=False)

    plot_df = plot_df[plot_df.thresh == 30].drop('thresh',1)
    plot_df = plot_df.set_index('year')
    plot_df.plot.bar()
    plt.show()

def qc_new_data(nat_new_loss, nat_old_loss, level):

    # left join old data to new data, so keep all records from old, and only matching from new.
    join_fields = ['iso', 'thresh', 'year']
    if level == 'subnat':
        join_fields += ['id1']
    merged = pd.merge(nat_old_loss, nat_new_loss, how='left', on=join_fields)
    
    # compare loss values between old and new
    merged['loss_dif'] = abs(merged['loss'] - merged['area'])
    
    # raise error if difference is significant. This is sum of loss at national level, in ha.
    if len(merged[merged.loss_dif > .05]) != 0:
        raise ValueError("there is a significant difference in loss between old and new data.")

    # second test. Compare number of records in new data 2001-2015.
    grouped_new_loss_df_filtered = nat_new_loss[nat_new_loss.year != 2016.0]
    len_new_less2016 = len(grouped_new_loss_df_filtered)
    
    len_old_data = len(nat_old_loss)
    
    if abs(len_old_data-len_new_less2016) != 0:
        raise ValueError("the number of iso/thresh/year records for matching years does not match.")
        
    # third test. Make sure the join has values in new data
    join_fields.remove('year')
    join_extent = pd.merge(nat_old_loss, nat_new_loss, how='left', on=join_fields)
    
    missing_vals = join_extent[join_extent['area'].isnull()]
    if len(missing_vals) != 0:
        print missing_vals
        raise ValueError("there are blanks in the 'area' field of the new data")
 
def main():

    parser = argparse.ArgumentParser(description='update umd carto tables with newest annual data')

    parser.add_argument('--level', '-l', required=True, choices=['nat', 'subnat'])

    args = parser.parse_args()
    
    level = args.level
    print "Updating carto for {}".format(level)    
    # connect to carto
    cc = cartoframes.CartoContext(base_url='https://wri-01.carto.com/', api_key=get_api_tokens())
    # read in the old carto table
    old_data = 'umd_{}_final_1'.format(level)
    old_loss_df = cc.read(old_data)

    print "reading in new loss"
    # read in the new csv data
    nat_new_loss = read_new_loss('gadm28_large_processed.csv', level)

    print "qc-ing the data"
    # qc the data
    qc_new_data(nat_new_loss, old_loss_df, level)   

    print "creating 2016 data"
    # write the new data, which means joining to extent levels to calc loss perc, etc.
    new_2016 = create_2016(nat_new_loss, old_loss_df, level)   

    print "appending new df to old"
    # append new df to old df.
    updated_loss = old_loss_df.append(new_2016, ignore_index=True)
    updated_loss[['thresh']] = updated_loss[['thresh']].apply(pd.to_numeric)

    print "writing to carto"

    updated_loss.to_csv("updated_loss.csv", encoding='utf-8')

    cc.write(updated_loss, 'umd_{}_staging'.format(level), overwrite=True)
    
if __name__ == main():
    main()
