## Hadoop loss tile processing

1. create a cluster
	- can clone an old one, or create a new one. currently using 16 d2.8xlarge machines, with all slaves running as spot instances with max price $1.50
	
2. once it's ready SSH in. If you want to see the spark console in your brower, use port forwarding (get to it via the links generated in the EMR part of the AWS console)

3. Run `annual_update.py`.
```
usage: annual_update.py [-h] --analysis-type {extent,loss,gain,biomass}
                        [--points-folder POINTS_FOLDER] --polygons-folder
                        POLYGONS_FOLDER --output-folder OUTPUT_FOLDER
```

##### What's this code doing?

This code will run the hadoop process, tabulating loss/extent from the points folder within polygons in the polygons folder.

If the analysis type is extent, it will iterate over the points in 10 degree latitude chunks to prevent out of memory errors.

The code will write different `application.properties` depending on the analysis type. For more information (and the scala source code) please see our fork of the (Spark PIP code)[https://github.com/wri/spark-pip/].

###### Application.properties example file -- LOSS
```
spark.app.name=YARN Points in World
output.path=hdfs:///user/hadoop/output
output.sep=,
points.y=1
points.path=s3a://gfw2-data/alerts-tsv/loss_2017/
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
