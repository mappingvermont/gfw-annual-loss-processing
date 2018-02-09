# gfw-tabulate-hadoop-loss-data

This process takes the output from our Spark-PIP zonal stats process and converts it to the cum-summmed data used by the flagship.

### Example input data:

After several false starts, we've finally settled on a common input/output format for our hadoop zonal stats processes.

The input polygons look like this:

| polyname | bound1 | bound2 | bound3 | bound4 | iso | adm1 | adm2 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| wdpa | --- | --- | --- | --- | NGA | 26 | 539 |
| bra_biomes | Cerrado | --- | --- | --- | BRA | 23 | 4351 |
| plantations | Pine | Industrial | --- | --- | USA | 17 | 212 |
| ifl_2013__wdpa | --- | --- | --- | --- | BRA | 23 | 4351 |

If we're processing loss data, the above table will have the following columns added:

| loss_year | thresh | loss_area | emissions |
| --- | --- | --- | --- |
| 5|15|202979.3358690382|0.003107805249357181
|12|10|2301.92479254|1.1405613065553998E-5
|10|30|9881.107236421|1.35012201795243E-4

And for extent, biomass, or gain data these columns will be tacked on to our polygon data.

| thresh | area |
| --- | --- |
|75|2342.111|
|20|898.90888|
|50|53802.10222|

The threshold values above need to be cum-summed for display on GFW-- our thresholds (e.g. loss where tree cover >=20) should tabulate all loss on areas where TCD was >= 20, not just the 20 - 30 range that the Spark PIP process outputs.

### Processing
Example command to process this data and save the output locally:
`python tabulate-and-push.py --input s3://gfw2-data/alerts-tsv/output/loss.csv`

### Historical precedent

At one point we managed this data as individual datasets in the API. One dataset for WDPA loss stats, one for gadm28, one for plantations, etc.

As such, there's a lot of legacy code to manage creation/overwriting of these datasets in the history of this repo. It's much easier to manage this as a single dataset, and elastic makes querying it easy, so this code is no longer needed.

### Postprocessing

Once we've processed loss, extent2000, and extent2010, we can join them using the `postprocess.py`.

The gain data has no threshold, and thus the raw data can be used without postprocessing it.

In addition to the above, we'll also need to include a CSV of the area of our polygons of interest (including the gadm28 base data as well).

We can generate this CSV using some code in the 1b section of this repo:

`python tabulate-bucket-area.py -b s3://gfw2-data/alerts-tsv/country-pages/ten-by-ten/`

This will download all the input data in the bucket, insert it into postgis, tabulate area_ha, and then write it to a CSV.

#### Specific file configuration

Given the complicated nature of joining these datasets, the input files must meet the following critiera:

- No duplicated records based on polyname/bound1/bound2/iso/adm1/adm2/thresh/year
- Extent2000 area field should be called area_extent_2000
- Extent2010 area field should be area
- Loss area field should be area, biomass should be called emissions
- *Gain is not postprocessed (no thresh to cumsum) and thus all gain_area should be divided by 10,000 to go from m2 to ha*
- Gain area field should be area_gain, and the thresh column should be removed (gain has no thresh anyway)
- The area field in the polygon area CSV should be area_poly_aoi


Once we have all these datasets lined up, we can run our postprocess script to generate our main summary table.

Here's an example of our final output:
[https://production-api.globalforestwatch.org/v1/query/499682b1-3174-493f-ba1a-368b4636708e?sql=SELECT * FROM data LIMIT 10](https://production-api.globalforestwatch.org/v1/query/499682b1-3174-493f-ba1a-368b4636708e?sql=SELECT%20*%20FROM%20data%20LIMIT%2010)
