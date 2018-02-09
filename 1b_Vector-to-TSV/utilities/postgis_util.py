import os
import logging
import subprocess

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


def add_cluster(table_name, cursor):

    # supposed to be good per https://gis.stackexchange.com/questions/19832/
    # and http://postgis.net/docs/performance_tips.html
    sql_list = ['analyze {}', 'CLUSTER {0}_geom_idx ON {0}']

    for sql in sql_list:
        logging.info(sql.format(table_name))
        cursor.execute(sql.format(table_name))


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

    conn, cursor = conn_to_postgis()
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
        conn, cursor = conn_to_postgis()
        close_conn = True

    # source: https://stackoverflow.com/questions/1874113/
    sql = "select exists(select * from information_schema.tables where table_name=%s)"
    cursor.execute(sql, (table_name,))

    table_exists = cursor.fetchone()[0]

    if close_conn:
        conn.close()

    return table_exists


def create_area_table():
    conn, cursor = conn_to_postgis()

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
               "area_poly_aoi double precision "
               ");")

        cursor.execute(sql)
        conn.commit()

    conn.close()


def insert_into_postgis(src_dataset, dummy_fields=None):

    creds = get_creds()
    conn_str = 'postgresql://{user}:{password}@{host}'.format(**creds)

    if os.path.splitext(src_dataset)[1] == '.shp':
        cmd = ['shp2pgsql']
    else:
        cmd = ['raster2pgsql', '-t', '512x512', '-I']

    cmd.extend(['-s', '4326', src_dataset, '|', 'psql', conn_str])

    # has to be string for some reason-- likely to do with the | required
    subprocess.check_call(' '.join(cmd), shell=True)

    table_name = os.path.splitext(os.path.basename(src_dataset))[0]

    # add these to match schema of other intersects
    if dummy_fields:

        # add dummy column names
        conn, cursor = conn_to_postgis()

        for field_dict in dummy_fields:
            field_name = field_dict.keys()[0]
            cursor.execute('ALTER TABLE {} ADD COLUMN {} varchar(30)'.format(table_name, field_name))
            cursor.execute("UPDATE {} SET {} = '1'".format(table_name, field_name))

        conn.commit()
        conn.close()

    return table_name


def check_col_exists(table_name, col_name, cursor):
    # source https://stackoverflow.com/a/31755654/4355916
    sql = ("SELECT EXISTS "
           " (SELECT 1 from information_schema.columns "
           "  WHERE table_name = '{}' AND column_name = '{}')".format(table_name, col_name))

    print sql
    cursor.execute(sql)

    return cursor.fetchall()[0][0]


def is_raster(table_name, cursor):
    return check_col_exists(table_name, 'rast', cursor)


def fix_geom(table_name, cursor, add_pkey=True):

    sql_list = ["UPDATE {} SET geom = ST_MakeValid(geom) WHERE ST_IsValid(geom) <> '1'",
                "UPDATE {} SET geom = ST_CollectionExtract(geom, 3)",
                "CREATE INDEX {0}_geom_idx ON {0} using gist(geom)"]

    if add_pkey:
        sql_list.extend(["ALTER TABLE {} ADD Column gid serial PRIMARY KEY"])

    for sql in sql_list:
        # logging.info(sql.format(table_name))
        cursor.execute(sql.format(table_name))

    add_cluster(table_name, cursor)


def export_to_shp(table_name, output_folder):

    output_shp = os.path.join(output_folder, table_name + '.shp')
    cmd = ['ogr2ogr', output_shp, build_ogr_pg_conn(), 'dialect', 'sqlite', '-sql', "SELECT * FROM {}".format(table_name)]
    subprocess.check_call(cmd)

    return output_shp


def conn_to_postgis():
    creds = get_creds()
    conn = psycopg2.connect(**creds)
    cursor = conn.cursor()

    return conn, cursor


def fix_raster_geom(table_name, cursor):

    sql_list = ["ALTER TABLE {} RENAME wkb_geometry to geom",
		"ALTER INDEX {0}_wkb_geometry_geom_idx RENAME TO {0}_geom_idx",
		"ALTER TABLE {} ADD COLUMN boundary_field2 integer",
		"UPDATE {} SET boundary_field2 = 1",
		"UPDATE {} SET geom = ST_CollectionExtract(ST_MakeValid(geom), 3) WHERE ST_IsValid(geom) <> '1'"]

    for sql in sql_list:
	cursor.execute(sql.format(table_name))
