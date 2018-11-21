# Summary AOIs to TSV

### Use case

In order to run zonal statistics on polygons and loss/biomass/gain using Hadoop, we need to prepare our geometry. This describes how to prepare boundaries (GADM and other boundaries, like primary forests and mining concessions) for intersection with forest loss, biomass, or gain. Preparing the geometry means converting shapefiles and rasters into TSVs.

In addition to converting input shapefiles to WKT-based TSV, this process will intersect input data with GADM boundaries (version of your choosing), grouping by any input attributes and dissolving by ISO/ADM1/ADM2. You can then use these TSVs as input for Hadoop zonal statistics.

### Install necessary packages on spot machine

In gfw-annual-loss-processing\1b_Summary-AOIs-to-TSV\requirements.txt, do `sudo pip install -r requirements.txt` to install the necessary packages on the spot machine.

### SHP --> TSV

##### Copy boundary shapefile to spot machine and then into PostGIS

First, get the shapefile onto the spot machine. Then import it to PostGIS. If your zonal statistics will use any of the fields in the shapefile (e.g., the specific biome in Brazil or the type of plantation), make sure to import that field into PostGIS and to maintain that field throughout the geometry correction process.

- Copy file: `aws s3 cp s3://gfw2-data/country/bra/zip/bza_biomes.zip .`
- Unzip file: `unzip bza_biomes.zip`
- View field names (optional): `ogrinfo -so -al bza_biomes.shp`
- Import the shapefile into a PostGIS table. This has some optional arguments.
- To not import any fields in the shapefile into PostGIS: `ogr2ogr -f "PostgreSQL" PG:"host=localhost" bza_biomes.shp -overwrite -progress -nln "bbm" -lco GEOMETRY_NAME=geom -nlt MULTIPOLYGON -t_srs EPSG:4326 -dialect sqlite -sql "SELECT Geometry FROM bza_biomes"`
- To import specific fields in the shapefile into PostGIS: `ogr2ogr -f "PostgreSQL" PG:"host=localhost" bza_biomes.shp -overwrite -progress -nln "bbm" -lco GEOMETRY_NAME=geom -nlt MULTIPOLYGON -t_srs EPSG:4326 -dialect sqlite -sql "SELECT Geometry, name FROM bza_biomes"`

##### Correct the geometry of the shapefile in PostGIS

Enter the Postgres shell: `psql`
View table attributes and fields in Postgres shell (optional but good idea): `\d+ bbm`

Then there are five steps to clean up the geometry of the table (shapefile imported to PostGIS) that you are going to convert to a tsv. Not all tables need these steps but they are always a good idea to do.

Correct the geometry of the table: `UPDATE bbm SET geom = ST_COLLECTIONEXTRACT(ST_MAKEVALID(geom), 3) WHERE ST_ISVALID(geom) <> '1';`

If you do not want to maintain any fields throughout the geometry correction process, do the following to fix geometry:
Explode the table into singlepart features: `CREATE TABLE bbm_explode AS SELECT (ST_DUMP(geom)).geom FROM bbm;`
Dissolve the singlepart features into one feature: `CREATE TABLE bbm_diss AS SELECT ST_UNION(geom) AS geom FROM bbm_explode;`
Re-explode the table into singlepart features: `CREATE TABLE bbm_explode2 AS SELECT (ST_DUMP(geom)).geom FROM bbm_diss;`

If you want to maintain specific fields throughout the geometry correction process so that it can be used in Hadoop (e.g., the names of the Brazil biomes or types of plantations), do the following instead of the explode-dissolve-explode commands above, replacing `name` with the name of the field you want to preserve:
`CREATE TABLE bbm_explode AS SELECT name, (ST_DUMP(geom)).geom FROM bbm;`
`CREATE TABLE bbm_diss AS SELECT name, ST_UNION(geom) AS geom FROM bbm_explode GROUP BY name;`
If done correctly, this should produce the number of distinct values in the fields you have saved. For example, if you wanted to preserve the biome name in the Brazil biomes file, this would produce `6`.
`CREATE TABLE bbm_explode2 AS SELECT name, (ST_DUMP(geom)).geom FROM bbm_diss;`

Re-correct the geometry of the table: `UPDATE bbm_explode2 SET geom = ST_COLLECTIONEXTRACT(ST_MAKEVALID(geom), 3) WHERE ST_ISVALID(geom) <> '1';`
This should produce `0`. `0` doesn't guarantee that the tsv process will work but it is a good sign.
If the above did not produce `0`, you can check whether there are any remaining geometry errors (optional): `SELECT count(*), ST_IsValid(geom) FROM bbm GROUP BY ST_IsValid;`
Exit the Postgres shell: `\q`

##### PostGIS table to TSV

This step cuts the postgres table into 10x10 degree tiles, intersects each tile with whatever version of GADM you supply, then uploads the processed tiles to the output directory where it can be used in the Hadoop process.

Generically: `python intersect-source-with-gadm.py --input-dataset name_of_postgres_table --zip-source gadm_file --output-name output_file`
Specific example: `python intersect-source-with-gadm.py --input-dataset bbm_explode2 --zip-source s3://gfw2-data/alerts-tsv/gis_source/gadm_3_6_adm2_final.zip --output-name bbm --s3-out-dir s3://gfw2-data/alerts-tsv/country-pages/climate/`

If this is the first time doing TSV conversion on this particular spot machine, `intersect-source-with-gadm.py` will download GADM to the spot machine and load it into PostGIS. However, it will only need to download and convert GADM once per spot machine. This will take several minutes (especially on the INSERT 0 1 phase of the conversion). Also, when GADM is importing to PostGIS, it will find some errors in the geometry. The shell will stop and ask you to run the displayed command in PostGIS shell (`UPDATE gadm_3_6_adm2_final SET geom = ST_CollectionExtract(ST_MakeValid(geom), 3) WHERE ST_IsValid(geom) <> '1';`) to correct the geometry, then it will process the UPDATE command for a long time. It is expected that it will show two notices about holes lying outside the shell at or near points X, Y; that is fine. Once it is done with the GADM geometry correction, rerun the above intersect-source-with-gadm.py command in the Linux shell and it should go smoothly.

If you want to export to TSV using a column in the shapefile/postgis (e.g., plantation type, Brazil biome), add --col-list <column names> as an argument. You can use up to two column names: `python intersect-source-with-gadm.py --input-dataset bbbm_explode2 --zip-source s3://gfw2-data/alerts-tsv/gis_source/gadm_3_6_adm2_final.zip --output-name bbm --s3-out-dir s3://gfw2-data/alerts-tsv/country-pages/climate/ --col-list name`

### Raster to TSV

`python intersect-source-with-gadm.py -i raster_in_filesystem.tif --n output_name`

This uses gdal_translate to chop the raster into 10x10 degress locally, then runs `raster2pgsql` to insert each into postgis. This is generaly runs pretty well, but can be subject to various issues that arise with using the postgis raster function. One import gotcha:

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

For polygons converted to tsvs:
Copy the output tsvs to your local computer.
Open the command prompt in gfw-annual-loss-processing\1b_Vector-to-TSV\utilities
Enter the Python shell and import the file decode_polygon_tsv.py: `import decode_polygon_tsv`
Convert the tsv into a vrt: `decode_polygon_tsv.build_vrt(r"C:\GIS\GFW_Climate_updates\bbm__10S_040W.tsv", r"C:\GIS\GFW_Climate_updates\bbm__10S_040W.vrt")`
Exit the Python shell and in the Windows command line convert the vrt into a GeoJSON (or shapefile would work, too): `ogr2ogr -f GeoJSON out.geojson C:\GIS\GFW_Climate_updates\bbm__10S_040W.vrt data`
Open QGIS and load the GeoJSON tile. Compare with the the 10x10 grid, GADM boundary, and non-administrative boundary that were used to make it.

Second, calculate percent difference of the loss/extent/gain values from this process as compared to other analysis methods. Output from this process is currently stored in the [country pages table](https://production-api.globalforestwatch.org/v1/dataset/499682b1-3174-493f-ba1a-368b4636708e). To QC, we'll pass geometries to the GFW API umd-loss-gain endpoint, and then compare the results to what's stored in the table. If necessary, an option using the python rasterstats package is included as well.

`python qc-output.py -n 100 -g 10 -s s3://gfw2-data/alerts-tsv/tsv-boundaries-tiled/ -o 499682b1-3174-493f-ba1a-368b4636708e`
