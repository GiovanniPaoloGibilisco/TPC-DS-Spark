package it.polimi.spark.tpcds;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class Config implements Serializable {

	/**
	 * generated serial version UID
	 */
	private static final long serialVersionUID = 5087417577620830639L;

	private static Config _instance;
	private static JCommander commander;

	static final Logger logger = LoggerFactory.getLogger(Config.class);

	private Config() {
	}

	public static Config getInstance() {
		if (_instance == null) {
			_instance = new Config();
		}
		return _instance;
	}

	public static void init(String[] args) {
		_instance = new Config();
		commander = new JCommander(_instance, args);
		logger.info("Configuration: --inputFile {} --outputFolder {} --runlocal {} --queryNumber {} --query {} --usage {}",
				new Object[] { _instance.inputFile, _instance.outputFolder, _instance.runLocal, _instance.queryId,
						_instance.query, _instance.usage });
	}

	@Parameter(names = { "-i",
			"--inputFile" }, required = true, description = "The hdfs folder containing the tables")
	public String inputFile;

	@Parameter(names = { "-o",
			"--outputFolder" }, required = true, description = "output folder to store the result of the query")
	public String outputFolder;

	@Parameter(names = { "-l", "--runLocal" }, description = "Use to run the tool in the local mode")
	public boolean runLocal = false;

	@Parameter(names = { "-is", "--queryId" }, description = "[optional] the id of the predefined query to run")
	public String queryId;

	@Parameter(names = { "-q", "--query" }, description = "[optional] the Hive Query to run")
	public String query;
	
	@Parameter(names = { "-u", "--usage" }, description = "print the help message")
	public boolean usage = false;;

	public void usage() {
		StringBuilder builder = new StringBuilder();
		commander.usage(builder);
		logger.info(builder.toString());
	}

}