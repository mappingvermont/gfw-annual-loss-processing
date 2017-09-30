from layer import Layer


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