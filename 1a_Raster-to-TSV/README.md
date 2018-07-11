## Writing loss tiles to TSV

##### Loss tile updates
This is never fun, but hopefully should take less than day to go from tif to TSV.

1. Update the source URL (points to Peter's data) and the destination S3 URL (something like s3://gfw2-data/alerts-tsv/loss_{{YEAR}}/ in `loss_tiles_to_tsv.py`.
2. Commit these changes
3. Start 13 m4.16xlarge large spot instances (we'll be splitting the tiles over all these machines)
4. SSH into each one and clone http://github.com/wri/gfw-annual-loss-processing and http://github.com/wri/raster-vector-to-tsv in the /home/ubuntu/ directory
5. TMUX!
6. run `python loss_tiles_to_tsv.py {{index}}` where {{index}} is an integer from 0 - 12


##### Writing extent and gain data
Extent data is a little bit harder to process given the massive size of the dataset (40,000 * 40,000) points in a single raster, in some cases. The `raster-vector-to-tsv` process was designed for sparsely populated rasters (GLAD, loss, etc), and doesn't handle dense ones very well.

I've hacked together something to process this that uses a previous version of the raster-vector-to-tsv repo.
For extent data, download this zip to a large spot machine:
s3://gfw2-data/alerts-tsv/batch-processing/treecover_process_v4.zip

For biomass, use:
s3://gfw2-data/alerts-tsv/batch-processing/sam-biomass-to-tsv.zip

These zips contain a script to run the code, and a version of `raster-vector-to-tsv` optimized for this sort of thing-- no raster joins, just writing the TCD or gain raster to TSV, calculating the area of each pixel, and uploading to s3. *Be sure to edit the code in job.py to modify the S3 output location*.

This situation will hopefully be fixed soon-- either by coming up with a better process, or by using GeoTrellis/GEE to work with rasters in their native format.
