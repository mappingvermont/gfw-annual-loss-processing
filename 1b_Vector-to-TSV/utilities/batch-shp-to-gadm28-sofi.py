import os
import subprocess
import logging
import psycopg2

import util
import postgis_util as pg_util

util.start_logging()

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sof_s3_dir = 's3://gfw-files/state_of_forest/indicator_data/'

files_to_intersect = [{'peat_all_single_dis2.zip': 'global_peat'}, {'mangrove_dis.zip':'mangroves'}, {'water_stress.zip': 'water_stress'}, {'wdpa_protected_areas_dis.zip': 'wdpa'}, {'Oil_palm_concessions_select_countries_dis.zip': 'oil_palm'}, {'mining_concessions_dis.zip':'mining'}, {'gfw_logging_single_dis.zip':'managed_forests'}, {'Resource_rights_select_countries_dis': 'resource_rights'}, {'land_rights_dis.zip':'land_rights'}, {'community_level_data_landmark_dis_prj.zip': 'landmark'}]

field_lookup = {'water_stress': 'OWR_cat'}

home_dir = '/home/ubuntu'
zipped_dir = os.path.join(home_dir, 'zip_data')
unzipped_dir = os.path.join(home_dir, 'unzipped_data')
cleaned_dir = os.path.join(home_dir, 'cleaned')

for folder in [zipped_dir, unzipped_dir, cleaned_dir]:
    if not os.path.exists(folder):
        os.mkdir(folder)

for f_dict in files_to_intersect:
    for f, tablename in f_dict.iteritems():
        
        s3_exists = False

        try:
            s3_source = os.path.join(sof_s3_dir, f)
            cmd = ['aws', 's3', 'cp', s3_source, zipped_dir]
            subprocess.check_call(cmd)
            s3_exists = True

        except subprocess.CalledProcessError:
            logging.error("{} must not exist on s3".format(f))
            s3_exists = False

        if s3_exists:
            #unzip
            zip_data = os.path.join(zipped_dir, f)
            subprocess.check_call(['unzip', zip_data, '-d', unzipped_dir])

            # connect to postgres
            creds = pg_util.get_creds()
            conn = psycopg2.connect(**creds)
            cursor = conn.cursor()
        
            # fix geom - insert into postgis
            unzip_path = os.path.join(unzipped_dir, table_name + '.shp')
            pg_util.insert_into_postgis(unzip_path, table_name)
        
            # explode multipolygons
            table_multi = table_name + "_multi"     
  
            try:
                uid_field = field_lookup[table_name]
                select_statement = 'SELECT {},'.format(uid_field)
            except KeyError:
                select_statement = 'SELECT'
            
            explode = 'CREATE TABLE {0} AS ({1} (ST_Dump(geom)).geom AS geom FROM {2});'.format(table_multi, select_statement, table_name)
            cursor.execute(explode)
        
            # fix geom types
            pg_util.fix_geom(table_multi, cursor)
        
            conn.commit()
            conn.close()
        
            # export to "cleaned" folder
            output_shp = pg_util.export_to_shp(table_multi, cleaned_dir)
        
            # run code
            z = 's3://gfw-files/sam/adm2_eco_watershed.zip'
            cmd = ['python', 'shp-to-gadm28-tiled-tsv.py', '-i', output_shp, '-z', z, '-n', table_name, '-s', 's3://gfw-files/sam/sof/']
        
            subprocess.check_call(cmd, cwd=root_dir)

