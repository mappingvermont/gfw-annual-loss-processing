import util

'''
 3 things we need

1. read in peter's csv to pandas

2. read in unprocesed loss.csv from hadoop output

3. read in lookup table of ISO | country name

Operating on Peter's table
- get year columns into 1 column (pd.melt?)
- make table so it looks like
country name | iso | year | area

Join Peter's table to ours in pandas df
country name | iso | year | peters area | hadoop area

on the side (not to be commited in this repo) --- make iso | countryname table
- get the gadm36 table into csv format ogr2ogr shape to csv

- use fuzzy lookup

(https://github.com/wri/gfw-annual-loss-processing/blob/d376899ee8e533e19e98a7c53d8fbe02addd25c9/1b_Vector-to-TSV/utilities/qc_util.py#L31)
or
https://stackoverflow.com/a/13680953/4355916

'''

peters_df = util.create_peters_df()

loss_df = util.create_loss_df()

calc_area_diff = util.calc_area_diff(peters_df, loss_df)

