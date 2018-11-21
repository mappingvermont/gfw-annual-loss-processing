## Writing loss-gain-extent-tiles to TSV

##### Loss tile updates
This is never fun, but hopefully should take less than day to go from tif to TSV.

1. Update the source URL (points to Peter's data) and the destination S3 URL (something like s3://gfw2-data/alerts-tsv/loss_{{YEAR}}/ in `loss_tiles_to_tsv.py`.
2. Commit these changes
3. Start 13 m4.16xlarge large spot instances (we'll be splitting the tiles over all these machines)
4. SSH into each one and clone http://github.com/wri/gfw-annual-loss-processing and http://github.com/wri/raster-to-tsv in the /home/ubuntu/ directory
5. TMUX!
6. run `python loss_tiles_to_tsv.py {{index}}` where {{index}} is an integer from 0 - 12


##### Writing extent and gain data
We don't have any code written here for extent updates, but now that we've improved our [raster-to-tsv]( http://github.com/wri/raster-to-tsv) code, we should be able to use it for extent and gain processes. To do this, use the loss_tiles_to_tsv.py code in this repo as a model for how to process the entire Hansen tile list.

##### Writing biomass tiles
The [gfw-climate-indicator-update repo](https://github.com/wri/gfw-climate-indicator-update) has a biomass-to-tsv.py script in it. This repo has an older copy of our [raster-to-tsv](https://github.com/wri/raster-to-tsv) committed within it, which seems strange. I'd recommend removing that stored repo from within it and adapting the biomas-to-tsv.py script it contains to use the updated version of raster-to-tsv.

### Quality Control of Output

How do we QC this stuff? Visual comparison of source data to TSV in QGIS.

For rasters converted to tsvs (biomass in 2000, forest extent/TCD in 2000):
Copy select output tsvs to your local computer.
Open the command prompt in gfw-annual-loss-processing\1a_Raster-to-TSV\utilities
Enter the Python shell and import the file decode_raster_tsv.py: `import decode_raster_tsv`
Convert the tsv into a vrt: `decode_raster_tsv.build_vrt(r"C:\GIS\GFW_Climate_updates\10S_040W_24601.tsv", r"C:\GIS\GFW_Climate_updates\10S_040W_24601.vrt")`
Exit the Python shell and in the Windows command line convert the vrt into a GeoJSON: `ogr2ogr -f GeoJSON out.geojson C:\GIS\GFW_Climate_updates\10S_040W_24601.vrt data`
Open QGIS and load the GeoJSON tile. Compare it with the the input raster used to make it. If the raster is biomass 2000 and it was masked by TCD30 for tsv conversion, load the TCD tile into QGIS and make sure that matches as expected.
