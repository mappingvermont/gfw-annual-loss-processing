# Vector to TSV

### Use case

After writing the loss and extent raster data to TSV, we need to prep our geometry for the hadoop zonal stats process.

In addition to converting input shapefiles to WKT-based TSV, this process will intersect input data with GADM boundaries (version of your choosing), grouping by any input attributes and dissolving by ISO/ADM1/ADM2.

### Install necessary packages on spot machine

In gfw-annual-loss-processing\1b_Vector-to-TSV\requirements.txt, do `sudo pip install -r requirements.txt`

### Copy boundary shapefile to spot machine and then into PostGIS

Copy file: `aws s3 cp s3://gfw2-data/country/bra/zip/bra_biomes.zip .`
Unzip file: `unzip bra_biomes.zip`
Import the shapefile into a PostGIS table. This has some optional arguments. It also doesn't save any fields in the shapefile: `ogr2ogr -f "PostgreSQL" PG:"host=localhost" bza_biomes.shp -overwrite -progress -nln "bbm" -lco GEOMETRY_NAME=geom -nlt MULTIPOLYGON -t_srs EPSG:4326 -dialect sqlite -sql "SELECT Geometry FROM bza_biomes"`

### Correct the geometry of the shapefile in PostGIS

Enter the Postgres shell: `psql`
Correct the geometry of the table: `UPDATE bbm SET geom = ST_COLLECTIONEXTRACT(ST_MAKEVALID(geom), 3) WHERE ST_ISVALID(geom) <> '1';`
Explode the table into singlepart features: `CREATE TABLE bbm_explode AS SELECT (ST_DUMP(geom)).geom FROM bbm;`
Dissolve the singlepart features into one feature: `CREATE TABLE bbm_diss AS SELECT ST_UNION(geom) AS geom FROM bbm_explode;`
Re-explode the table into singlepart features: `CREATE TABLE bbm_explode2 AS SELECT (ST_DUMP(geom)).geom FROM bbm_diss;`

NOTE: If you want to maintain some field throughout the geometry correction process so that it can be used in Hadoop (e.g., the names of the Brazil biomes), do the following instead of the explode-dissolve-explode commands above, replacing `name` with the name of the field you want to preserve:
`CREATE TABLE bbm_explode AS SELECT name, (ST_DUMP(geom)).geom FROM bbm;`
`CREATE TABLE bbm_diss AS SELECT name, ST_UNION(geom) AS geom FROM bbm_explode GROUP BY name;`
`CREATE TABLE bbm_explode2 AS SELECT name, (ST_DUMP(geom)).geom FROM bbm_diss;`

Re-correct the geometry of the table: `UPDATE bra_explode2 SET geom = ST_COLLECTIONEXTRACT(ST_MAKEVALID(geom), 3) WHERE ST_ISVALID(geom) <> '1';`
Exit the Postgres shell: `\q`

### PostGIS table to TSV

Generically: `python shp-to-gadm28-tiled-tsv.py -i name_of_postgres_table --n output_name`
Specific example: `python shp-to-gadm28-tiled-tsv.py --input-dataset bbbm_explode2 --zip-source s3://gfw2-data/alerts-tsv/gis_source/gadm_3_6_adm2_final.zip --output-name bbm --s3-out-dir s3://gfw2-data/alerts-tsv/country-pages/climate/`

This will cut the postgres table into 10x10 degree tiles, intersect each tile with GADM28, then upload the processed tiles to the output directory where it can be used in the Hadoop process. Make sure to change the output directory to the correct directory.

If this is the first time doing tsv conversion on this particular spot machine, downloading and importing the GADM file into PostGIS will take several minutes (especially on the INSERT 0 1 phase). It will only need to do that once per spot machine. Also, when GADM is importing to PostGIS, it will find some errors in the geometry. The shell will stop and ask you to run the displayed command in PostGIS shell (UPDATE gadm_3_6_adm2_final SET geom = ST_CollectionExtract(ST_MakeValid(geom), 3) WHERE ST_IsValid(geom) <> '1';) to correct the geometry, although it will process the UPDATE command for a long time. It it expected that it will show two notices about holes lying outside the shell at or near points X, Y; that is not a problem. Once it is done with the GADM geometry correction, rerun the above shape-to-gadm28 command in the Linux shell and it should go smoothly. 

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
