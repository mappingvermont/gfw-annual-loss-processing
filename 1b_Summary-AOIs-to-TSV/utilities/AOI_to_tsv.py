import subprocess
import pandas as pd

def shp_to_csv(shp, name_field):

    shp_name = shp[:-4]

    print "Converting", shp_name, "from shp to csv..."

    # Converts the shapefile to a csv
    cmd = ['ogr2ogr', '-f', 'CSV', '{}.csv'.format(shp_name), '{}.shp'.format(shp_name), '-lco', 'GEOMETRY=AS_WKT',
           '-overwrite', '-progress', '-t_srs', 'EPSG:4326', '-SQL', 'SELECT {0} FROM {1}'.format(name_field, shp_name)]
    subprocess.check_call(cmd)

    print "Converting", shp_name, "from csv to tsv..."

def csv_to_tsv(shp_name):

    # For csvs that aren't too large, they are read into Pandas all at once
    try:

        # Formats the csv correctly for input to Hadoop and outputs the expected tsv
        in_file = pd.read_csv('{}.csv'.format(shp_name))

    # For csvs that are too large to be read into Pandas all at once, they are read in line by line
    except:

        mylist = []

        for chunk in pd.read_csv('soy16_15.csv', sep=';', chunksize=1):

            mylist.append(chunk)

        in_file = pd.concat(mylist, axis=0)

    file_formatted = in_file
    # file_formatted = in_file['WKT']
    file_formatted['bound1'], file_formatted['bound2'], file_formatted['bound3'], file_formatted['bound4'], \
    file_formatted['iso'], file_formatted['adm1'], file_formatted['adm2'], file_formatted['extra'] = \
        [1, 1, 1, 1, 'ZZZ', '1', '1', '1']
    file_formatted_head = file_formatted.head(100)
    print list(file_formatted_head.columns.values)
    file_formatted.to_csv('{}.tsv'.format(shp_name), sep='\t', index=False, header=False)