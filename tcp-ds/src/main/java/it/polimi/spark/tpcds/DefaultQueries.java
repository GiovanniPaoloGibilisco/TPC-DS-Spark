package it.polimi.spark.tpcds;

import java.util.ArrayList;
import java.util.List;

public class DefaultQueries {

	
	private static List<String> queries;
	
	private static void init(){
		queries = new ArrayList<String>();
		
		//Query #1
		queries.add("SELECT * from call_center;");
		
		//TODO: ADD query
	}
	
	public static String getQuery(int queryNumber){
		if (DefaultQueries.queries == null)
			DefaultQueries.init();
		return queries.get(queryNumber-1);
	}

}
