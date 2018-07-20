## Hadoop loss tile processing

1. Create a cluster
	- can clone an old one, or create a new one. currently using 16 d2.8xlarge machines, with all slaves running as spot instances with max price $1.50

2. Once it's ready, SSH in. You can get the connection string/hostname in the AWS console. It'll be in the format of hadoop@ec2-54-234-230-206.compute-1.amazonaws.com. If you want to see the spark console in your browser, use port forwarding (get to it via the links generated in the EMR part of the AWS console)

3. Install git: `sudo yum install git`

4. Install tmux: `sudo yum install tmux`

5. Clone https://github.com/wri/gfw-annual-loss-processing to the spot machine using: `git clone https://github.com/wri/gfw-annual-loss-processing`

6. Change directories (cd) into gfw-annual-loss-processing/1c_Hadoop-Processing

7. Install python libraries: `sudo pip install -r requirements.txt`

8. To see arguments for Hadoop running code, inside 1c_Hadoop-Processing: `python annual_update.py`

9. Run `annual_update.py`.
```
usage: annual_update.py [-h] --analysis-type {extent,loss,gain,biomass}
                        --points-folder POINTS_FOLDER --polygons-folder
                        POLYGONS_FOLDER --output-folder OUTPUT_FOLDER
                        [--dryrun] --iterate-by {points,polygons,None}
                        [{points,polygons,None} ...]
```

Example dry-run for annual forest loss/emissions: `python annual_update.py --analysis-type loss --points-folder s3://gfw2-data/alerts-tsv/loss_2017/ --output-folder s3://gfw2-data/alerts-tsv/output2017/20180711/climate/raw/loss/ --polygons-folder s3://gfw2-data/alerts-tsv/country-pages/climate/ --iterate-by None --dryrun`

Example actual run for annual forest loss/emissions: `python annual_update.py --analysis-type loss --points-folder s3://gfw2-data/alerts-tsv/loss_2017/ --output-folder s3://gfw2-data/alerts-tsv/output2017/20180711/climate/raw/loss/ --polygons-folder s3://gfw2-data/alerts-tsv/country-pages/climate/ --iterate-by None`

Example dry-run for forest extent in 2000: `python annual_update.py --analysis-type extent --points-folder s3://gfw2-data/alerts-tsv/extent_2000/ --output-folder s3://gfw2-data/alerts-tsv/output2017/20180717/climate/raw/extent/ --polygons-folder s3://gfw2-data/alerts-tsv/country-pages/climate/ --iterate-by points --dryrun`

Example actual run for forest extent in 2000: `python annual_update.py --analysis-type extent --points-folder s3://gfw2-data/alerts-tsv/extent_2000/ --output-folder s3://gfw2-data/alerts-tsv/output2017/20180717/climate/raw/extent/ --polygons-folder s3://gfw2-data/alerts-tsv/country-pages/climate/ --iterate-by points`  
(Note: Because of the --iterate-by points argument, this will produce one output csv for each 10 deg latitude band (00N, 10N, 10S, etc.). These csvs will be automatically combined in post-processing.)

To see the configuration file: `more application.properties`. The row points.path shows what latitude band the forest extent processing is on. 




##### What's this code doing?

This code will run the hadoop process, tabulating loss/extent/gain/biomass from the points folder within polygons in the polygons folder.

If the analysis type is extent, use `--iterate-by points` to iterate over the points in 10 degree latitude chunks to prevent out of memory errors.

The code will write different `application.properties` depending on the analysis type. For more information (and the scala source code) please see our fork of the (Spark PIP code)[https://github.com/wri/spark-pip/].

###### Example application.properties example file -- LOSS
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

###### Example application.properties example file -- EXTENT
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
