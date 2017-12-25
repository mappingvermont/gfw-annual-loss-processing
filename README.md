<<<<<<< HEAD
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
=======
#### Hansen Tiles

Hansen Tiles are generated from an EarthEngine asset whose various bands contain the raw Hansen lossyear-data masked by various thresholds of treecover2000.  This same asset is used for the Hansen Analysis API (note tests of the API using this image along with derivative `ImageCollections` can be found [here](https://gist.github.com/brookisme/ff6f557aeb84870e5827c78a5c7ba8f7).

This repo contains:

* [hansen\_ee\_processing/js/composite_asset.js](#hasset): javascript code to produce the most recent version of the Hansen Asset described above
* [hansen\_ee\_processing/python/hansen_tiles.py](#htiles): python script that generates the hansen tiles (in 2 steps).

---
<a name='hasset'></a>
#### Hansen Composite 14-15
To re-generate this asset simply run the code. The current [Hansen Asset](https://code.earthengine.google.com/?asset=projects/wri-datalab/HansenComposite_14-15) is unique because it merges data from Hansen 2014 and Hansen 2015. Therefore process probably won't need to be repeated.  That said, the second half of the code, starting around [here](https://github.com/wri/hansen_ee_processing/blob/master/js/composite_asset.js#L52) could easily be modified to create future assets.


---
<a name='htiles'></a>
#### Hansen Tiles

Due to the earthengine limits discussed [here](https://groups.google.com/forum/#!topic/google-earth-engine-developers/wU4NNoWTD70) tile processing happens in 2 (and a half) steps:

1. Export Tiles for zoom-levels 12-7, and export an earthengine asset for zoom-level 7
2. Export Tiles for zoom-levels 6-2

The code can be run via the command line:

```bash
# step 1
$ python hansen_tiles.py {threshold} inside

# step 2 (after the earthengine asset for zoom-level 7 has completed processing)
$ python hansen_tiles.py {threshold} outside
```

There are various options like using test geometries, versioning, changing the zoom-level used as a break between step 1 and 2, and more.

```bash
python|master $ python hansen_tiles.py -h
usage: hansen_tiles.py [-h] [-g GEOM_NAME] [-v VERSION]
                       threshold {inside,outside,zasset} ...

HANSEN COMPOSITE

positional arguments:
  threshold             treecover 2000: one of [10, 15, 20, 25, 30, 50, 75]
  {inside,outside,zasset}
    inside              export the zoomed in z-levels
    outside             export the zoomed out z-levels
    zasset              export z-level to asset

optional arguments:
  -h, --help            show this help message and exit
  -g GEOM_NAME, --geom_name GEOM_NAME
                        geometry name (https://fusiontables.google.com/DataSou
                        rce?docid=13BvM9v1Rzr90Ykf1bzPgbYvbb8kGSvwyqyDwO8NI)
  -v VERSION, --version VERSION
                        version
 ```


>>>>>>> hansen_ee_processing/master
