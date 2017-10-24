## Hadoop loss tile processing


1. create a cluster
	- can clone an old one, or create a new one. currently using 13 d2.8xlarge machines, with all slaves running as spot instances with max price $1.50
	
2. once it's ready SSH in. If you want to see the spark console in your brower, use port forwarding (get to it via the links generated in the EMR part of the AWS console)

3. Install gdal using `gdal_install.sh`.  This may be buggy, so be sure to run `ogrinfo` when completed to test.

4. Make sure all the data you want to analyze is in s3://gfw2-data/alerts-tsv/tsv-boundaries-gadm28/

5. Edit annual_update.py and the various utilities to change the input directory from loss_2016 to `loss_{current_year}` and output directory from `output2016` to `output{current_year}`

6. Run `/usr/bin/python annual_update.py -a loss` to process loss data for all data in the above s3 directory


##### What's this code doing?

This code will iterate over every polygon in the TSV directory, grabbing it's extent using `ogrinfo` and writing an `application.properties` file. It will then call spark-pip, process the file, and upload the output to the proper folder on s3.

###### Application.properties example file -- LOSS
```
spark.app.name=YARN Points in World
output.path=hdfs:///user/hadoop/output
output.sep=,
points.y=1
points.path=s3a://gfw2-data/alerts-tsv/loss_2016/
points.fields=2,3,4,5
points.x=0
reduce.size=0.5
polygons.path=s3a://gfw2-data/alerts-tsv/tsv-boundaries-climate/
polygons.wkt=0
polygons.fields=1,2,3,4,5,6,7,8
analysis.type=loss
```

###### Application.properties example file -- EXTENT
```
spark.app.name=YARN Points in World
output.path=hdfs:///user/hadoop/output
output.sep=,
points.y=1
points.path=s3a://gfw2-data/alerts-tsv/extent_2000/00N* 
points.fields=2,3
points.x=0
reduce.size=0.5
polygons.path=s3a://gfw2-data/alerts-tsv/tsv-boundaries-climate/
polygons.wkt=0
polygons.fields=1,2,3,4,5,6,7,8
analysis.type=extent
```
