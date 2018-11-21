#### GFW Pro raster data

GFW Pro uses AWS lambda functions to provide dynamic analysis of loss / gain / extent rasters. When the loss data updates (and when GFW Pro is ready to add this data), we'll need to do the following.

#### A few things to confirm
1. Get the location of the gfwpro staging bucket from Blue Raster. The live data lives here: `s3://gfwpro-raster-data`, but you'll need to work with them to determine an appropriate staging bucket. **NB**: The gfwpro-raster-data bucket is private; you'll need an access key to list it if you'd like to view the data.
2. Previously AWS recommended using a random hash in front of all S3 objects (for us this looks like `f002ee-10S_040E_loss_at_30tcd.tif`) but apparently [this is no longer required](https://aws.amazon.com/about-aws/whats-new/2018/07/amazon-s3-announces-increased-request-rate-performance/). Confirm with Blue Raster that hashing is no longer necessary.

#### Write the actual raster data
1. Start a large spot machine for our process
2. Modify the included `mask_loss_w_tcd.py` code to point to the correct source data, specify your threshold of interest (10, 30, or 90% tcd for gfwpro work), and the BR staging output location on s3.
3. Run it for tcd thresholds 10, 30 and 90.

#### Update the staging version of the loss analysis code
1. Work with Blue Raster to determine the best workflow for updating the staging version of the lambda function without interfering with the production system.
2. The gfwpro analysis code lives here (private repo): https://github.com/blueraster/gfwpro-analyses
3. You'll likely need to build new VRTs and index.geojson files for each set of thresholded loss data (10, 30 and 90 tcd). These VRTs and geojson should overwrite the loss VRTs and geojsons referenced here: https://github.com/blueraster/gfwpro-analyses/tree/develop/src/geoprocessing/raster-analysis/data.
4. Remember, coordinate with Blue Raster to ensure that this deployment only goes to staging, and not to production until they're fully ready to incorporate the newest round of loss data.
