## Writing loss tiles to TSV

##### Loss tile updates
This should be a pretty straightforward process.

1. Start a pretty large spot instance (m4.16xlarge is probably best)
2. SSH in, and clone http://github.com/wri/raster-vector-to-tsv
3. Create the proper directory structure used in `loss_tiles_to_tsv.py`
4. Update the code to make sure download and upload (S3) URLs are correct
5. Run it!


##### Writing extent and gain data
Extent data is a little bit harder to process given the massive size of the dataset (40,000 * 40,000) points in a single raster, in some cases. The `raster-vector-to-tsv` process was designed for sparsely populated rasters (GLAD, loss, etc), and doesn't handle dense ones very well.

I've hacked together something to process this, currently stored here:
s3://gfw2-data/alerts-tsv/batch-processing/treecover_process_v4.zip

It contains a script to run the code, and a version of `raster-vector-to-tsv` optimized for this sort of thing-- no raster joins, just writing the TCD or gain raster to TSV, calculating the area of each pixel, and uploading to s3. *Be sure to edit the code in job.py to modify the S3 output location*.

This situation will hopefully be fixed soon-- either by coming up with a better process, or by using GeoTrellis/GEE to work with rasters in their native format.
