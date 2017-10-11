import os
import subprocess


def get_creds():

    try:
        creds = {'host': os.environ['PG_HOST'],
                 'user': os.environ['PG_USER'],
                 'password': os.environ['PG_PASS'],
                 'dbname': os.environ['PG_DBNAME']}

    except KeyError:
        creds = {'host': 'localhost', 'password': 'gis', 'dbname': 'gis', 'user': 'gis'}

    return creds


def find_tile_overlap(layer_a, layer_b):

    print 'finding tile overlap'

    # looks at the s3 directory to figure out what tiles both "layers" have in common based on tile id strings


def export(table_name, tile, creds):

    shp_dir = os.path.join(tile.out_dir, 'shp')

    if not os.path.exists(shp_dir):
        os.mkdir(shp_dir)

    shp_filename = tile.tile_id + '.shp'

    to_shp_cmd = ['pgsql2shp', '-u', creds['user'], '-P', creds['password'], '-h', 'localhost',
                  creds['dbname'], table_name.lower(), '-f', shp_filename]
    print to_shp_cmd

    # for some reason can't specify a full output path, just a filename
    # to choose dir, set it to CWD
    subprocess.check_call(to_shp_cmd, cwd=shp_dir)

    out_shp = os.path.join(shp_dir, table_name.lower() + '.shp')
    out_geojson = os.path.join(tile.out_dir, tile.tile_id + '.geojson')

    # this will ultimately be a TSV, using geojson for now to QC
    to_geojson_cmd = ['ogr2ogr', '-f', 'GeoJSON', out_geojson, out_shp]
    print to_geojson_cmd

    subprocess.check_call(to_geojson_cmd)


def boundary_field_to_sql(field_name):

    if 'boundary_field' in field_name:
        field_name = '1 AS ' + field_name

    return field_name


def table_has_rows(cursor, table_name):

    has_rows = False

    # source: https://stackoverflow.com/questions/4138734/
    cursor.execute('SELECT count(*) FROM (SELECT 1 FROM {} LIMIT 1) AS t'.format(table_name))

    if cursor.fetchone()[0]:
        has_rows = True

    return has_rows





