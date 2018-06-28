# Vector to TSV

### Use case

After writing the loss and extent raster data to TSV, we need to prep our geometry for the hadoop zonal stats process.

In addition to converting input shapefiles to WKT-based TSV, this process will intersect input data with GADM28 boundaries, grouping by any input attributes and dissolving by ISO/ADM1/ADM2.

### Simple shapefile to TSV

`python shp-to-gadm28-tiled-tsv.py -i name_of_postgres_table --n output_name`

This will cut the postgres table into 10x10 degree tiles, intersect each tile with GADM28, then upload the processed tiles to the output directory where it can be used in the hadoop process.

### Raster to TSV

`python shp-to-gadm28-tiled-tsv.py -i raster_in_filesystem.tif --n output_name`

This is the same as above, but uses gdal_translate to chop the raster into 10x10 degress locally, then runs `raster2pgsql` to insert each into postgis. This is generaly runs pretty well, but can be subject to various issues that arise with using the postgis raster function. One import gotcha:

**Input tifs must not have the nodata flag set!**

I have no idea why this ^^ matters, but it seems to result in topology errors when nodata is set. To get around this, I recommend having your nodata pixels be 0, which will be filtered out by the raster intersect. Check utilities/geop.py for more info.

### Intersect tiled TSVs

Many of our zonal stats use-cases involve multiple layers-- for example  how much loss occurred in protected areas that are within primary forest. This code identifies overlapping tiles for two datasets, brings them into PostGIS, then intersects them and writes the output tiles to S3.

`python intersect-tiled-tsvs.py -a name_of_dataset_a -b name_of_dataset_b -n output_name -r source_s3_bucket -s destination_s3_bucket`

Example:

`python intersect-tiled-tsvs.py -a primary_forest -b wdpa -n primary_forest__wdpa -r s3://gfw2-data/alerts-tsv/ -s s3://gfw2-data/alerts-tsv/`

### TSV to Tiled TSV

Much of our boundary data is already TSV'd from previous hadoop analyses. This code pulls an existing TSV from S3, tiles it, then writes the output to S3.

`python tsv-to-tiled-tsv.py -i s3://path/to/data.tsv -n output_name`

### Tabulate Bucket Area

After running all the intersections, we need to tabulate the area of each individual AOI. This code downloads all TSVs in an input S3 path, inserts them into PostGIS, and tabulates area for each in ha-- required for our final output stats.

`python tabulate-bucket-area.py -b s3://path/to/data/`

### Quality Control of Output

How do we QC this stuff? Definitely lots of moving parts here. First, visual comparison of source data to TSV in QGIS.

Second, calculate percent difference of the loss/extent/gain values from this process as compared to other analysis methods. Output from this process is currently stored in the [country pages table](https://production-api.globalforestwatch.org/v1/dataset/499682b1-3174-493f-ba1a-368b4636708e). To QC, we'll pass geometries to the GFW API umd-loss-gain endpoint, and then compare the results to what's stored in the table. If necessary, an option using the python rasterstats package is included as well.

`python qc-output.py -n 100 -g 10 -s s3://gfw2-data/alerts-tsv/tsv-boundaries-tiled/ -o 499682b1-3174-493f-ba1a-368b4636708e`
