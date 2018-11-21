#### Loss API updates
Work with the Vizz team to update our [GFW Analysis GEE](https://github.com/gfw-api/gfw-analysis-gee) code to point to the new loss asset on GEE.

This asset will be created by the 4_GEE-Tiles process in this repo, so the only update we'll hopefully need to do is point the GEE microservice to the new location. Currently, this asset is specified here:
https://github.com/gfw-api/gfw-analysis-gee/blob/master/gfwanalysis/config/base.py#L18
