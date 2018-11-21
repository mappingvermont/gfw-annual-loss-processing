import subprocess

import wget


def mask_loss(tile_id):
    '''
    mask hansen loss tile to a tcd threshold
    '''

    # set masking threshold
    tcd_threshold = 30

    # loss source location, including {} for tile_id
    loss_loc = 'http://glad.geog.umd.edu/Potapov/GFW_2016/tiles_2016/{}.tif'

    # tcd source location
    tcd_loc = 'http://commondatastorage.googleapis.com/earthenginepartners-hansen/GFC2014/Hansen_GFC2014_treecover2000_{}.tif'

    # move up raster to new location on s3
    s3_location = 's3://gfw2-data/alerts-tsv/temp-output/hansen_2016_masked_{}tcd/'.format(tcd_threshold)

    loss_tile = '{}_loss.tif'.format(tile_id)
    tcd_tile = '{}_tcd.tif'.format(tile_id)

    download_dict = {loss_loc: loss_tile, tcd_loc: tcd_tile}

    for source, new_name in download_dict.iteritems():
        source = source.format(tile_id)
        new_name = new_name.format(tile_id)
        cmd = ['wget', source, '-O', new_name]
        
        print cmd
        subprocess.check_call(cmd)
               
    # gdal calc statement to create raster of loss at our tcd of interest
    calc = '--calc=A*(B>{})'.format(tcd_threshold)

    masked_loss =  "{0}_loss_at_{}tcd.tif".format(tile_id, tcd_threshold)
    outfile = '--outfile={}'.format(masked_loss)

    cmd = ['gdal_calc.py', '-A', loss_tile, '-B', tcd_tile, calc, outfile, '--NoDataValue=0', 
            '--co', 'COMPRESS=DEFLATE', '--co', 'TILED=YES']
    subprocess.check_call(cmd)

    # move raster to our output location
    subprocess.check_call(['aws', 's3', 'mv', masked_loss, s3_location])

    # delete loss and tcd raster
    subprocess.check_call(['rm', loss_tile])
    subprocess.check_call(['rm', tcd_tile])


