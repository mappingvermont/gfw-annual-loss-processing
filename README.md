# gfw-tabulate-hadoop-loss-data

Tabulate loss data at various polygon boundaries using the standard GFW cumsum approach.

### Example input data:
http://gfw2-data.s3.amazonaws.com/alerts-tsv/output/wdpa_protected_areas_diss_int_diss.csv

The above cryptic filename refers to Hansen 2000 - 2015 loss pixels that occur within WDPA boundaries. These WDPA boundaries are intersected with GADM2.8 boundaries, meaning the input CSV looks like this: 
| ISO | ADM1 | ADM2 | Loss Year | Loss Threshold | Pixel Area | Loss Emissions |
| --- | ---- | ---- | --------- | -------------- | ---------- | -------------- |

### Processing
Example command to process this data (using this repo) and save the results as a dataset in the GFW-API:
`python tabulate-and-push.py --input s3://gfw2-data/alerts-tsv/output/wdpa.csv --environment staging --tags wdpa test`

This will create a new dataset in the staging API with the tags `wdpa` and `test`, using the CSV on S3 as a source. Fun!

### Output
Example output data:
[http://staging-api.globalforestwatch.org/query/21e23320-3b63-424a-a669-393f10d451b8?sql=SELECT * FROM data LIMIT 10](http://staging-api.globalforestwatch.org/query/21e23320-3b63-424a-a669-393f10d451b8?sql=SELECT%20*%20FROM%20data%20LIMIT%2010)
