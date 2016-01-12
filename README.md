How to install Spark with Hive support and run tpcds queries:


1) Install Hadoop and configure hdfs

2) Compile Spark with hive support

git clone https://github.com/apache/spark
(eventually move to the desired tag)

./make-distribution.sh --name spark-with-hive --tgz -Psparkr -Phadoop-2.6 -Dhadoop.version=2.6.2 -Phive -Phive-thriftserver -Pyarn
(check that the version of hadoop you are using is the same used in the command to build spark, this takes a while...)

Install Spark in some folder (e.g. /opt/spark extracting the generated archive)

3) Install hive 
	- Configure hive to use mysql as metastore http://www.cloudera.com/content/www/en-us/documentation/archive/cdh/4-x/4-2-0/CDH4-Installation-Guide/cdh4ig_topic_18_4.html
	- skip the part of the configuration about the "hive.metastore.uris" parameter
	- Add in the folder with the spark configuration the same hive-site.xml file used in hive configuration (this tells spark where to look for the metastore)
	
4) Generate tpcds dataset (I'm using https://github.com/hortonworks/hive-testbench)
	- if not alreadygenerated on hdfs, put it there

5) Load the tables in hive to setup the metastore using script: reset-metastore.sh <scale> <data location on hdfs>
	- e.g. ./reset-metastore.sh 2 /data
	
6) Build the spark application with the embedded queries
	- git clone https://github.com/GiovanniPaoloGibilisco/TPC-DS-Spark
	- cd tcp-ds
	- mvn clean install

7) run the query submitting the application to spark
	- spark-submit --master spark://clusterino1:7077 --class it.polimi.spark.tpcds.Query target/uber-tcp-ds-0.0.1-SNAPSHOT.jar -i /data/2 -o /output -db tpcds_text_2 -id R1
	- the db is the one created in step 5, the name is "tpcds_text_"+<scale>
	- custom queries can be executed using -q "query text" instead of the -id argument
	
To change the dataset size:

1) Repeat Step 4 
2) Repeat Step 5 (optionally dropping the other database)
3) Repeat Step 7 (as many time as needed with the required queries)