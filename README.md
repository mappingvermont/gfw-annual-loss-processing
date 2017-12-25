## Processing the Annual Hansen Data

We all love the annual Hansen forest change data, but how the heck do we update it across the platform? This repo will attempt to provide a one-stop-shop for this momentous task.

#### Data Prep

##### 1a - Raster to TSV
We first need to write the loss tiles to TSV, joining them to tree cover density and biomass rasters, and calculating area for each pixel. Output TSVs are saved to S3.

##### 1b - Hadoop Processing
Next, we need to tabulate the loss data using a modified version of Mansour Raad's [spark-pip](https://github.com/wri/spark-pip/) application.

##### 1c - Data Prep GEE
To write tiles and respond to analysis requests, we need to create an additional GEE asset from the base Hansen data.


#### Postprocessing

##### 2 - Cumulate Results and Create API Datasets
Before we share the output data from Hadoop, we need to tabulate cum sum values for all the loss thresholds. We can then use this as an input to create spreadsheets, and to make data available through the API for various charts on the country pages.


##### 3 - Create Summary Spreadsheets
Take the cumsummed data and package it into user-friendly spreadsheets-- one at the iso/adm1 level for all countries, and then one for each country that includes iso/adm1/adm2 data.

##### 4 - Write GEE tiles
Write the updated loss tiles using EarthEngine.

##### 5 - Update the loss analysis API to include 2016 data
Should be pretty straightforward after we create the new asset; just need to update the code to point at it and test

##### 6 - Update Carto with new years data
Need to update the cartoframes code to handle the Country name (utf-8 encoding) and setting index to False
  Lib\sit-packages\cartogrames\context.py -- line 311:
  df.drop(geom_col, axis=1, errors='ignore').to_csv(tempfile) -> df.drop(geom_col, axis=1, errors='ignore').to_csv(tempfile, encoding='utf-8', index=False)
