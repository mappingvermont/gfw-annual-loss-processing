import os

from sqlalchemy import create_engine
import psycopg2


def get_creds():
    try:
        creds = {'host': os.environ['PG_HOST'],
                 'user': os.environ['PG_USER'],
                 'password': os.environ['PG_PASS'],
                 'dbname': os.environ['PG_DBNAME']}

    except KeyError:
        creds = {'host': 'localhost', 'password': 'gis', 'dbname': 'gis', 'user': 'gis'}

    return creds


def build_ogr_pg_conn():
    creds = get_creds()

    return 'PG:user={user} password={password} dbname={dbname} host={host}'.format(**creds)


def sqlalchemy_engine():
    creds = get_creds()

    return create_engine('postgresql://{user}:{password}@{host}'.format(**creds))


def find_admin_columns(cursor, tile1, tile2):

    tile_with_cols = None
    iso_col_list = ['iso', 'id_1', 'id_2']

    for tile_id, t in zip(('a', 'b'), (tile1, tile2)):
        cursor.execute("select column_name from information_schema.columns WHERE table_name=%s", (t.postgis_table,))
        col_names = [row[0] for row in cursor]

        # check if iso col list is part of the larger table column list
        if set(iso_col_list).issubset(set(col_names)):
            tile_with_cols = tile_id

    if not tile_with_cols:
        raise ValueError('Neither {} or {} have iso, id_1, id_2 columns'.format(tile1.postgis_table, tile2.postgis_table))

    # a.ISO, a.ID_1, a.ID_2
    return [tile_with_cols + '.' + x for x in iso_col_list]


def drop_table(tablename):

    creds = get_creds()
    conn = psycopg2.connect(**creds)

    cursor = conn.cursor()
    cursor.execute('DROP TABLE {}'.format(tablename))

    conn.commit()
    conn.close()


def boundary_field_dict_to_sql_str(field_list):
    out_list = []

    for field_dict in field_list:
        out_list += ['{} AS {}'.format(k, v) for k, v in field_dict.iteritems()]

    return ', '.join(out_list)


def table_has_rows(cursor, table_name):
    has_rows = False

    # source: https://stackoverflow.com/questions/4138734/
    cursor.execute('SELECT count(*) FROM (SELECT 1 FROM {} LIMIT 1) AS t'.format(table_name))

    if cursor.fetchone()[0]:
        has_rows = True

    return has_rows


def check_table_exists(table_name, cursor=None):
    # if we are sending a cursor, assume we want it back. So don't close conn
    close_conn = False

    if not cursor:
        creds = get_creds()
        conn = psycopg2.connect(**creds)

        cursor = conn.cursor()
        close_conn = True

    # source: https://stackoverflow.com/questions/1874113/
    sql = "select exists(select * from information_schema.tables where table_name=%s)"
    cursor.execute(sql, (table_name,))

    table_exists = cursor.fetchone()[0]

    if close_conn:
        conn.close()

    return table_exists


def create_area_table():
    creds = get_creds()
    conn = psycopg2.connect(**creds)

    cursor = conn.cursor()

    if not check_table_exists('aoi_area', cursor):
        sql = ("CREATE TABLE aoi_area ( "
               "polyname character varying, "
               "boundary_field1 character varying, "
               "boundary_field2 character varying, "
               "boundary_field3 character varying, "
               "boundary_field4 character varying, "
               "iso character varying, "
               "id_1 character varying, "
               "id_2 character varying, "
               "area_ha double precision "
               ");")

        cursor.execute(sql)
        conn.commit()

    conn.close()

    
 def insert_into_postgis(src_shp, table_name, dummy_fields=None):

    creds = pg_util.get_creds()
    
    conn_str = 'postgresql://{user}:{password}@{host}'.format(**creds)

    cmd = ['shp2pgsql', '-I', '-s', '4326', src_shp, '|', 'psql', conn_str]

    # has to be string for some reason-- likely to do with the | required
    subprocess.check_call(' '.join(cmd), shell=True)

    # add dummy column names
    conn = psycopg2.connect(**creds)
    cursor = conn.cursor()

    # add these to match schema of other intersects
    for field_dict in dummy_fields:
        field_name = field_dict.keys()[0]
        cursor.execute('ALTER TABLE {} ADD COLUMN {} varchar(30)'.format(table_name, field_name))
        cursor.execute("UPDATE {} SET {} = '1'".format(table_name, field_name))

    conn.commit()
    conn.close()
    
    
def fix_geom(table_name, cursor):

    sql = "UPDATE {} SET geom = ST_MakeValid(geom) WHERE ST_IsValid(geom) <> '1'".format(table_name)
    cursor.execute(sql)

    # remove linestings and points from collections
    sql = "UPDATE {} SET geom = ST_CollectionExtract(geom, 3)".format(table_name)
    cursor.execute(sql)

    
def export_to_shp(table_name, output_folder):

    output_shp = os.path.join(output_folder, table_name + '.shp')
    cmd = ['ogr2ogr', output_shp, build_ogr_pg_conn(), '-sql', '"SELECT * FROM {}"'.format(table_name)]
    
    subprocess.check_call(cmd)
    
    return output_shp
