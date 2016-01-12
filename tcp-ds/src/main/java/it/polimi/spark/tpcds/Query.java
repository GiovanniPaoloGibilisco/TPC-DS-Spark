package it.polimi.spark.tpcds;

import java.io.IOException;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query {

	static Config config;
	static FileSystem hdfs;
	static final Logger logger = LoggerFactory.getLogger(Query.class);
	static SQLContext sqlContext;
	static final int PEEK_SIZE = 10;

	public static void main(String[] args) throws IOException {

		Configuration hadoopConf = new Configuration();
		hdfs = FileSystem.get(hadoopConf);

		// the spark configuration
		SparkConf conf = new SparkConf().setAppName("QueryExecutor");
		// the configuration of the application (as launched by the user)
		Config.init(args);
		config = Config.getInstance();

		if (config.usage) {
			config.usage();
			return;
		}

		if (config.runLocal) {
			conf.setMaster("local[1]");
		}

		// either -n or -q has to be specified
		if (config.queryId == null && config.query == null) {
			logger.info(
					"Either the number of the default query (-n) or the code of the query (-q) has to be specified");
			config.usage();
			return;
		}

		// just one between -n or -q has to be specified
		if (config.queryId != null && config.query != null) {
			logger.info("You have specified the execution of the predefined query " + config.queryId
					+ " and the execution of a custom query. Please specify just one.");
			config.usage();
			return;
		}

		JavaSparkContext sc = new JavaSparkContext(conf);

		sqlContext = new HiveContext(sc.sc());
		if (hdfs.exists(new Path(config.outputFolder)))
			hdfs.delete(new Path(config.outputFolder), true);

		// TODO change this to load the tables (which format? which schema?
		/*
		 * DataFrame logsframe = sqlContext.read().orc(config.inputFile);
		 * logsframe.cache(); logsframe.registerTempTable("call_center");
		 * 
		 * 
		 * //debugging logsframe.show(); logsframe.printSchema();
		 */

		FileStatus[] tableFolders = hdfs.listStatus(new Path(config.inputFile));

		logger.info("Looking for data in: " + config.inputFile);
		for (FileStatus tableFolder : tableFolders) {
			if (tableFolder.isDirectory()) {
				String tableName = tableFolder.getPath().getName();
				logger.info("Importing Table: " + tableName + "from: " + tableFolder.getPath());
				sqlContext.sql("import table " + tableName + " from '" + tableFolder.getPath().toString() + "'");
			}
		}

		String query;
		if (config.queryId != null)
			query = DefaultQueries.getQuery(config.queryId);
		else
			query = config.query;

		logger.info("Running Query: " + query);
		StopWatch timer = new StopWatch();
		timer.start();
		DataFrame result = sqlContext.sql(query);
		timer.split();
		logger.info("Query Executed: " + timer.getSplitTime());
		result.write().orc(config.outputFolder);
		timer.split();
		logger.info("Output Written: " + timer.getSplitTime());
		logger.info("Result tuples count: " + result.count());
		Row[] peek = result.head(PEEK_SIZE);

		for (Row row : peek)
			logger.info(row.toString());

		sc.close();
		logger.info("Finished");

	}

}
