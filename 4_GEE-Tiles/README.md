#### Hansen Tiles

Hansen Tiles are generated from an EarthEngine asset whose various bands contain the raw Hansen lossyear-data masked by various thresholds of treecover2000.  This same asset is used for the Hansen Analysis API (note tests of the API using this image along with derivative `ImageCollections` can be found [here](https://gist.github.com/brookisme/ff6f557aeb84870e5827c78a5c7ba8f7).

This repo contains:

* [hansen\_ee\_processing/js/composite_asset.js](#hasset): javascript code to produce the most recent version of the Hansen Asset described above
* [hansen\_ee\_processing/python/hansen_tiles.py](#htiles): python script that generates the hansen tiles (in 2 steps).

---
<a name='hasset'></a>
#### Hansen Composite
Before we can generate any tiles, we'll need to build a composite GEE asset with the new Hansen data. This is used in tile generation, and in dynamic user-drawn AOI analysis. To re-generate this asset simply go the GEE playground and run js/composite_asset.js, making sure to update `var h17` (the input) and `var COMPOSITE_IMG_NAME` (the output).

---
<a name='htiles'></a>
#### Hansen Tiles

Due to the earthengine limits discussed [here](https://groups.google.com/forum/#!topic/google-earth-engine-developers/wU4NNoWTD70) tile processing happens in 2 (and a half) steps:

1. Update hansen_tiles.py to point to the composite asset generated above and proper tile output directories
2. Export Tiles for zoom-levels 12-7, and export an earthengine asset for zoom-level 7
3. Export Tiles for zoom-levels 6-2

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

### Final update step

After tiles are created, make sure to update the umd-loss-gain API to use the new Hansen composite asset. The path to the composite asset in EE should be updated here: https://github.com/gfw-api/gfw-analysis-gee/blob/master/gfwanalysis/config/base.py


