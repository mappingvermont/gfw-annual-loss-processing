# Vector to TSV

### Use case

After writing the loss and extent raster data to TSV, we need to prep our geometry for the hadoop zonal stats process.

In addition to converting input shapefiles to WKT-based TSV, this process will intersect input data with GADM28 boundaries, grouping by any input attributes and dissolving by ISO/ADM1/ADM2.

### Simple shapefile to TSV

`python shp-to-gadm28-tiled-tsv.py -i /path/to/input.shp --n output_name`

This will cut the input shapefile into 10x10 degree tiles, intersect each tile with GADM28, then upload the processed tiles to `s3://gfw2-data/alerts-tsv/tsv-boundaries-tiled/` where it can be used in the hadoop process.

### Intersect tiled TSVs

Many of our zonal stats use-cases involve multiple layers-- for example  how much loss occurred in protected areas that are within primary forest. This code identifies overlapping tiles for two datasets, brings them into PostGIS, then intersects them and writes the output tiles to S3.

`python intersect-tiled-tsvs.py -a name_of_dataset_a -b name_of_dataset_b -n output_name`

### TSV to Tiled TSV

Much of our boundary data is already TSV'd from previous hadoop analyses. This code pulls an existing TSV from S3, tiles it, then writes the output to S3.

`python tsv-to-tiled-tsv.py -i s3://path/to/data.tsv -n output_name`

### Tabulate Bucket Area

After running all the intersections, we need to tabulate the area of each individual AOI. This code downloads all TSVs in an input S3 path, inserts them into PostGIS, and tabulates area for each in ha-- required for our final output stats.

`python tabulate-bucket-area.py -b s3://path/to/data/`