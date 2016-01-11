package it.polimi.spark.tpcds;

import java.util.HashMap;
import java.util.Map;

public class DefaultQueries {

	private static Map<String,String> queries;

	private static void init() {
		queries = new HashMap<String,String>();

		// Query #Q1
		queries.put("Q1","select avg(ss_quantity), avg(ss_net_profit) " 
				+ "from store_sales,catalog_sales "
				+ "where cs_bill_customer_sk = ss_customer_sk and ss_quantity > 10 and ss_net_profit > 0 "
				+ "limit 100");

		// Query #Q2
		queries.put("Q2","select a.aq "
				+ "from (select cs_item_sk, avg(cs_quantity) as aq "
				+ "from catalog_sales "
				+ "where cs_quantity > 2 "
				+ "group by cs_item_sk) a "
				+ "join (select i_item_sk,i_current_price from item where i_current_price > 2 order by i_current_price) b on a.cs_item_sk = b.i_item_sk "
				+ "order by a.aq "
				+ "limit 100");
		
		// Query #Q3
		queries.put("Q3","select avg(ws_quantity), avg(ws_ext_sales_price), avg(ws_ext_wholesale_cost), sum(ws_ext_wholesale_cost)"
				+ " from web_sales "
				+ "where (web_sales.ws_sales_price between 100.00 and 150.00) or (web_sales.ws_net_profit between 100 and 200)"
				+ " limit 100");

		// Query #Q4
		queries.put("Q4","select inv_item_sk, inv_warehouse_sk "
				+ "from inventory "
				+ "where inv_quantity_on_hand > 10 "
				+ "group by inv_item_sk,inv_warehouse_sk having sum(inv_quantity_on_hand)>20 "
				+ "order by inv_warehouse_sk "
				+ "limit 100");
		
		// Query #R1
		queries.put("R1","select avg(ws_quantity), avg(ws_ext_sales_price), avg(ws_ext_wholesale_cost), sum(ws_ext_wholesale_cost)"
				+ "from web_sales"
				+ "where (web_sales.ws_sales_price between 100.00 and 150.00) or (web_sales.ws_net_profit between 100 and 200)"
				+ "group by ws_web_page_sk"
				+ "limit 100");

		// Query #R2
		queries.put("R2","select inv_item_sk,inv_warehouse_sk "
				+ "from inventory where inv_quantity_on_hand > 10 "
				+ "group by inv_item_sk,inv_warehouse_sk having sum(inv_quantity_on_hand)>20"
				+ "limit 100");
		
		// Query #R3
		queries.put("R3","select avg(ss_quantity), avg(ss_net_profit)"
				+ "from store_sales "
				+ "where ss_quantity > 10 and ss_net_profit > 0"
				+ "group by ss_store_sk having avg(ss_quantity) > 20"
				+ "limit 100");

		// Query #R4
		queries.put("R4","select cs_item_sk, avg(cs_quantity) as aq "
				+ "from catalog_sales "
				+ "where cs_quantity > 2"
				+ "group by cs_item_sk");
				
		// Query #R5
		queries.put("R5","select inv_warehouse_sk, sum(inv_quantity_on_hand) "
				+ "from inventory "
				+ "group by inv_warehouse_sk having sum(inv_quantity_on_hand) > 5"
				+ "limit 100");

		

		// TODO: ADD query
	}

	public static String getQuery(String queryId) {
		if (DefaultQueries.queries == null)
			DefaultQueries.init();
		return queries.get(queryId);
	}

}
