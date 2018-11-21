## Processing the Annual Hansen Data

We all love the annual Hansen forest change data, but how the heck do we update it across the platform? This repo will attempt to provide a one-stop-shop for this momentous task.

#### Data Prep

##### 1a - Loss-Gain-Extent to TSV
Use this code to write our "point" raster datasets (loss / gain / extent) to TSV. Per specific requirements for each dataset, rasters can be joined and pixel area calculations added. Output TSVs are saved to S3.

##### 1b - Summary-AOIs-to-TSV
Intersect our summary aois (IFL, WDPA, primary forest, etc) with GADM and convert to tsv. The inputs to this workflow can be either vector (imported into PostGIS) or raster (a standard TIF).

##### 1c - Hadoop Processing
Next, we need to tabulate the loss data using a modified version of Mansour Raad's [spark-pip](https://github.com/wri/spark-pip/) application.

##### 1d - Data Prep GEE
To write tiles and respond to analysis requests, we need to create an additional GEE asset from the base Hansen data.

#### Postprocessing

##### 2 - Cumulate Results and Create API Datasets
Before we share the output data from hadoop, we need to tabulate cum sum values for all the loss thresholds. We can then use this as an input to create spreadsheets, and to make data available through the API for various charts on the country pages.

##### 3 - Create Summary Spreadsheets
Take the cumsummed data and package it into user-friendly spreadsheets-- one at the iso/adm1 level for all countries, and then one for each country that includes iso/adm1/adm2 data.

##### 4 - Write GEE tiles
Write the updated loss tiles using EarthEngine.

##### 5 - Update the loss analysis API to include 2018 data
Should be pretty straightforward after we create the new asset; just need to update the code to point at it and test

##### 6 - Update GFW Pro raster data
After consulting with Blue Raster, we need to build COG tifs (`-co TILED=YES -co COMPRESS=DEFLATE`) of the new loss data masked by 10%, 30% and 90% threshold values. outputs will go here: s3://gfwpro-raster-data/.
