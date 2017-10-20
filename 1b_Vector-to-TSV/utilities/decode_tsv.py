import os
import subprocess


def decode(input_layer):

    download_tsv(input_layer)

    input_tsv = os.path.join(input_layer.layer_dir, 'data.tsv')
    output_vrt = os.path.join(input_layer.layer_dir, 'data.vrt')

    vrt = build_vrt(input_tsv, output_vrt)

    input_layer.input_dataset = vrt


def download_tsv(input_layer):

    # download TSV locally
    local_tsv = os.path.join(input_layer.layer_dir, 'data.tsv')
    cmd = ['aws', 's3', 'cp', input_layer.input_dataset, local_tsv]

    subprocess.check_call(cmd)

    # update input_dataset to point to our local tsv
    input_layer.input_dataset = local_tsv


def build_vrt(input_tsv, output_vrt):

    ogr_layer_name = os.path.splitext(os.path.basename(input_tsv))[0]

    vrt_text = '''<OGRVRTDataSource>
        <OGRVRTLayer name="data">
            <SrcDataSource>{}</SrcDataSource>
            <SrcLayer>{}</SrcLayer>
            <GeometryType>wkbPolygon</GeometryType>
            <GeometryField encoding="WKT" field='field_1'/>
        </OGRVRTLayer>
    </OGRVRTDataSource>'''.format(input_tsv, ogr_layer_name)

    # vrt_path = os.path.join(input_layer.layer_dir, 'data.vrt')
    with open(output_vrt, 'w') as thefile:
        thefile.write(vrt_text)

    # we'll then use this to get our aoi bounds
    # for this to work, we need to uncomment the VRT driver in fiona source
    # somewhere near here: /usr/local/lib/python2.7/dist-packages/fiona/drvsupport.py
    # uncomment and change it from VRT to OGR_VRT
    # then we can get extent using fiona and not ogr2ogr

    return output_vrt
