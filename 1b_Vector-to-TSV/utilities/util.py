import os
import subprocess
import psycopg2


def download_hansen_footprint():

    print 'downloading hansen footprint to data folder'
    # hansen footprint source:
    # s3://gfw2-data/alerts-tsv/gis_source/lossdata_footprint.geojson

def find_tile_overlap(layer_a, layer_b):

    print 'finding tile overlap'

    # looks at the s3 directory to figure out what tiles both "layers" have in common based on tile id strings

def export(table_name, tile):

    shp_dir = os.path.join(tile.out_dir, 'shp')

    if not os.path.exists(shp_dir):
        os.mkdir(shp_dir)

    shp_filename = tile.tile_id + '.shp'
    to_shp_cmd = ['pgsql2shp', 'charlie', table_name.lower(), '-f', shp_filename]
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

def postgis_intersect(tile):

    # conn = psycopg2.connect("host=localhost dbname=gis user=gis password=gis")
    conn = psycopg2.connect("host=localhost dbname=charlie user=charlie password=charlie")
    cursor = conn.cursor()

    groupby_columns = ['ISO', 'ID_1', 'ID_2']
    groupby_columns = ", ".join(groupby_columns)
    bbox = ', '.join([str(x) for x in tile.bbox])

    table_name = '{}_{}'.format(tile.dataset_name, tile.tile_id)


    sql = ("CREATE TABLE {table_name} AS "
           "SELECT {fields}, ST_MakeValid(ST_Union(ST_Intersection(ST_MakeValid(c.geom), ST_MakeValid(b.geom)))) as geom "
           "FROM (SELECT {fields}, ST_MakeValid(ST_Union(ST_MakeValid(ST_Intersection(a.geom, bbox.geom)))) as geom "
           "      FROM adm2_final a "
           "      JOIN (select ST_MakeEnvelope({bbox}) as geom) bbox "
           "      ON ST_Intersects(a.geom, bbox.geom) "
           "      GROUP BY {fields}) AS c, "
           "{in_data} b "
           "WHERE ST_Intersects(c.geom, b.geom) AND ST_GeometryType(c.geom) IN ('ST_Polygon', 'ST_MultiPolygon')"
           "GROUP BY {fields} ".format(fields=groupby_columns, in_data=tile.dataset_name, bbox=bbox, table_name=table_name))
    print sql

    cursor.execute(sql)
    conn.commit()

    # source: https://stackoverflow.com/questions/4138734/
    check_table_sql = cursor.execute('SELECT count(*) FROM(SELECT 1 FROM {} LIMIT 1) AS t'.format(table_name))

    if cursor.fetchone()[0]:
        export(table_name, tile)

    cursor.execute('DROP TABLE {}'.format(table_name))
    conn.commit()

    conn.close()


