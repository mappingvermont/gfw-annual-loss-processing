import subprocess
from osgeo import ogr
import os


def download_hansen_footprint():

    print 'downloading hansen footprint to data folder'
    # hansen footprint source:
    # s3://gfw2-data/alerts-tsv/gis_source/lossdata_footprint.geojson

def find_tile_overlap(layer_a, layer_b):

    print 'finding tile overlap'

    # looks at the s3 directory to figure out what tiles both "layers" have in common based on tile id strings


def intersect_layers(layer_a, layer_b):

    # maybe create a new layer called layer_c with it's own output directory?

    # this should be multithreaded somehow too
    for tile_id in layer_a.tile_list:

        print 'intersecting tile id {} for {} and {}'.format(tile_id, layer_a.source, layer_b.source)

        # layer_a and layer_b have the same tile list
        # write VRT so that it can deal with the TSV format that the data is in currently
    '''<OGRVRTDataSource>
        <OGRVRTLayer name="layer_a">
            <SrcDataSource>{0}.tsv</SrcDataSource>
            <SrcLayer>{0}</SrcLayer>
            <GeometryType>wkbPolygon</GeometryType>
            <GeometryField encoding="WKT" field='field_1'/>
        </OGRVRTLayer>
        <OGRVRTLayer name="layer_b">
            <SrcDataSource>{0}.tsv</SrcDataSource>
            <SrcLayer>{0}</SrcLayer>
            <GeometryType>wkbPolygon</GeometryType>
            <GeometryField encoding="WKT" field='field_1'/>
        </OGRVRTLayer>
        </OGRVRTDataSource>'''

        # once you have that for both, do ST_Union
        # ogr2ogr out_path.tsv data.vrt -dialect sqlite
        # -sql "SELECT ST_Union(a.geom, b.geom), <unique field name>, a.iso, a.adm1, a.adm2 FROM layer_a a, layer_b b"

def run_subprocess(cmd):

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    subprocess_list = []

    # Read from STDOUT and raise an error if we parse one from the output
    for line in iter(p.stdout.readline, b''):

        if 'id (String) =' in line:
            subprocess_list.append(line.split("=")[-1:][0].strip())

    print '\n'.join(['-'*30] + subprocess_list + ['-'*30])

    return subprocess_list

    
def get_extent(inShapefile):
    
    # Get a Layer's Extent

    inDriver = ogr.GetDriverByName("ESRI Shapefile")
    inDataSource = inDriver.Open(inShapefile, 0)
    inLayer = inDataSource.GetLayer()
    extent = inLayer.GetExtent()

    llx = str(extent[0])
    urx = str(extent[1])
    lly = str(extent[2])
    ury = str(extent[3])
    
    ulx = '17.5933497022' # xmin, llx
    lry = '-1.49411937225'#  ymin, lly
    lrx = '21.2033510458' # xmax, urx
    uly = '1.43291359209' # ymax, ury
    return llx, urx, lly, ury
    
    
def make_vrt(geom_a, geom_b):
    # make vrt:
    tiles = geom_a
    tiles_layer = os.path.basename(tiles).split(".")[0]
    layer_name = os.path.basename(geom_b).split(".")[0]

    vrt_text = '''<OGRVRTDataSource>
        <OGRVRTLayer name="{0}">
        <SrcDataSource>{1}</SrcDataSource>
        <SrcLayer>{0}</SrcLayer>
        </OGRVRTLayer>
        <OGRVRTLayer name="{2}">
        <SrcDataSource>{3}</SrcDataSource>
        <SrcLayer>{2}</SrcLayer>
        </OGRVRTLayer>
        </OGRVRTDataSource>'''.format(layer_name, geom_b, tiles_layer, tiles)

    the_vrt = 'data.vrt'
    with open(the_vrt, 'w') as thefile:
        thefile.write(vrt_text)
        
    return the_vrt
        
        
        
def intersect_with_gadm28(tile):
    the_vrt = make_vrt(r'U:\sgibbes\gadm28_adm2_final\gadm28_adm2_final\adm2_final.shp', tile.dataset)
    # use ogr to intersect gadm and geometry and limit to the extent of the bbox
    groupby_columns = ['ISO', 'ID_1', 'ID_2']
    groupby_columns = ", ".join(groupby_columns)
    
    sql = 'SELECT ST_Intersection(A.geometry, B.geometry) AS geometry, {0} from adm2_final a, {1} b WHERE ST_INTERSECTS(a.geometry, b.geometry)'.format(groupby_columns, tile.dataset_name)
    bbox = [str(x) for x in tile.bbox]
    cmd = ['ogr2ogr', '-sql', sql, '-dialect', 'SQLITE', tile.output_file, the_vrt, '-clipsrc'] + bbox
    print cmd
    subprocess.check_call(cmd)
    # download the correct gadm28 tile
    # write VRT so it reads gadm28 TSV correctly
    # intersect
    