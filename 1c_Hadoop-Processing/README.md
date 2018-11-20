## Hadoop loss tile processing

1. Clone an existing Haddop cluster:
	- Go to the AWS console and go to EMR.
	- can clone an old one, or create a new one. currently using 16 d2.8xlarge machines, with all slaves running as spot instances with max price $1.50
	- For large jobs (like global annual loss/emissions processing), clone a machine that had >10000 run hours. They all have 15 servant/slave/helper machines.
	- For small jobs, 2 servant/slave/helper machines is enough.

2. Once it's ready (several minute wait), SSH in. You can get the connection string/hostname in the AWS console. It'll be in the format of hadoop@ec2-54-234-230-206.compute-1.amazonaws.com. If you want to see the spark console in your browser, use port forwarding (get to it via the links generated in the EMR part of the AWS console)

3. Install git: `sudo yum install git`

4. Install tmux: `sudo yum install tmux`

5. Clone https://github.com/wri/gfw-annual-loss-processing to the spot machine using: `git clone https://github.com/wri/gfw-annual-loss-processing`

6. Change directories (cd) into gfw-annual-loss-processing/1c_Hadoop-Processing

7. Install python libraries: `sudo pip install -r requirements.txt`

8. To see arguments for Hadoop running code, inside 1c_Hadoop-Processing: `python annual_update.py`

9. Run `annual_update.py`. For large jobs, you can do what's shown below.
For small jobs (using <15 servant/slave/helper machines, probably for GLAD or fire alerts (things where there aren't values for most pixels)), you need to decrease the executor memory that's expected; small machines don't have the memory that's expected by default for large jobs. To do that, cd into gfw-annual-loss-processing/1c_Hadoop-Processing/utilities and enter annual_helpers.py using `nano annual_helpers.py`. Change the executor memory argument in the subprocess call to `9g` from 20g. Press ctrl X to exit and save your changes. If you don't change the executor memory, you'll get a lengthy error about available memory and Hadoop won't run. Then proceed with running `annual_update.py` as shown below.

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

###### Example application.properties example file -- CUMULATIVE CARBON GAIN
```
spark.app.name=YARN Points in World
output.path=hdfs:///user/hadoop/output
output.sep=,
points.y=1
points.path=s3a://gfw2-data/climate/carbon_model/model_output_tsv/20181116/cumulGain_tcd2000/00N*
points.fields=2,3,4
points.x=0
reduce.size=0.5
polygons.path=s3a://gfw2-data/alerts-tsv/tsv-boundaries-climate/
polygons.wkt=0
polygons.fields=1,2,3,4,5,6,7,8
analysis.type=cumulGain
```

### Adding new datatypes to Hadoop

Hadoop expects certain data in certain columns. If you want to put other data through Hadoop or do something different with it, you need to add a new datatype to Hadoop.
These methods were developed to process some of the outputs of the global forest carbon model through Hadoop because they didn't conform to the existing types of data that our Hadoop process could handle.

1. Modify Hadoop code on local computer in a few places to handle new data type
    1. https://github.com/wri/spark-pip/blob/master/src/main/scala/com/esri/Summarize.scala (object Summary and adding new def and case class at bottom) and https://github.com/wri/spark-pip/blob/master/src/main/scala/com/esri/MainApp.scala (val validAnalyses). 
    2. Changes can be based on processLoss. In Pycharm, do ctrl+shift+f to find all instances of "loss", copy those lines, and change the new versions accordingly.
2. Git push to spark-pip repo.
3. Create test tsvs of rasters you want to put through Hadoop using the raster-to-tsv repo. Note that the raster-to-tsv repo may require modification to create the datatypes that you want to Hadoop.
4. Create tsvs of a polygon that intersects the test tsv area you want to use for testing. This can be GADM tsvs (since it has global coverage) or use the below steps to create a small shape for your test area.
    1. For test polygons, create some geojsons in the area you're interested in at geojson.io (e.g., http://geojson.io/#map=10/9.7767/-2.2055)
	2. Save the geojson into a txt locally and change the extension to geojson.
	3. Convert the geojson to a csv:  ogr2ogr -f csv out.csv boundaries.geojson -lco geometry=as_wkt
	4. Correct the format of the csv and convert it to a tsv in pandas locally.
	5. df = pd.read_csv("out.csv")
	6. >>> df['iso'] = 'GHA'
	7. >>> df['adm1'] = 1
	8. >>> df['adm2'] = 1
	9. >>> df['name'] = 'test'
	10. >>> df['bound2'] = 1
	11. >>> df['bound3'] = 1
	12. >>> df['bound4'] = 1
	13. >>> df['last'] = 1
	14. >>> df = df[['WKT', 'name', 'ID', 'bound2', 'bound3', 'bound4', 'iso', 'adm1', 'adm2', 'last']]
	15. df.to_csv('test_boundary.tsv', header=None, sep='\t', index=False)
5. Create m4.large spot machine using spotutil.
6. Clone the spark-pip repo: `git clone https://github.com/wri/spark-pip`
7. On spot machine: `sudo apt install maven` It may take 5-6 minutes for the spot machine to be ready to install this (i.e. if there's a message like E: Could not get lock..., just wait a few more minutes).
8. Change directories into spark-pip: `cd spark-pip`
9. Try compiling maven on the spot machine from within spark-pip: `mvn compile` (this will take several minutes the first time because it needs to download lots of stuff). It will not be able to compile because the spot machine doesn't have spark.
10. Change directory back to home: `cd ..`
11. Download spark from one of the mirrors here https://www.apache.org/dyn/closer.lua/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz using wget: `wget http://apache.cs.utah.edu/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz`
12. Install spark on spot machine, following these directions through step 3: https://datawookie.netlify.com/blog/2017/07/installing-spark-on-ubuntu/. Make sure to use the correct version of spark for the commands throughout.
	1. `tar -xvf spark-2.4.0-bin-hadoop2.7.tgz`
	2. `sudo mv spark-2.4.0-bin-hadoop2.7 /usr/local/`
	3. `sudo ln -s /usr/local/spark-2.4.0-bin-hadoop2.7/ /usr/local/spark`
	4. `cd /usr/local/spark`
	5. `export SPARK_HOME=/usr/local/spark`
13. Change directory to the root and add spark to the spot machine path. To do this, do: `nano ~/.bashrc`. Below the gdal export line, add `export PATH=$PATH:/usr/local/spark/bin/` (which means “set my PATH to whatever my path is, plus this new spark dir”).
14. In the home directory type `source .bashrc` to update the spot machine paths to what's now in bashrc. 
15. Type `echo $PATH` to make sure that spark is now in the PATH.
16. Change directory to spark-pip folder.
17. Type `mvn compile` to compile the scala, to make sure it is compiling alright.
18. Type `mvn package` to compile and build the scala.
19. Run Hadoop on the test inputs to make sure everything is running alright: `./local.sh` (runs the commands in that script in bash)
20. Change local.properties to the correct inputs: polygons.path=file:///, points.path=file:///, points.fields (2,3,4,5 if one variable of interest and two associated variables (TCD and loss year), 2,3,4 if one variable of interest and one associated variable (TCD)), output.path=file:///, analysis.type
21. Run Hadoop on the input file: `./local.sh`
22. cd to data/output/
23. `cat *.csv` to see all output combined. Make sure they are summed by the correct properties.
24. If the output was correct, copy the new .jar file in the target folder to s3 by zipping the folder. The annual-loss-processing repo will use the zipped jar for its analysis, so any changes you make to the spark-pip repo won't affect a big Hadoop cluster until you put the compiled Scala code on s3 for Hadoop clusters to get it from. 
	1. `zip -r target.zip target/`
	2. Copy target.zip to s3: `aws s3 cp target.zip s3://gfw2-data/alerts-tsv/target_0.3.zip`. If just adding new features to Hadoop, it's okay to replace 0.3 with another 0.3. Nightly/weekly Hadoop uses target_0.3.zip.

