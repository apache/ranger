package com.xasecure.hbase.client;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HBaseClientTester {

	private static final Log LOG = LogFactory.getLog(HBaseClientTester.class) ;

	public static void main(String[] args) throws Throwable {

		HBaseClient hc = null;

		if (args.length <= 2) {
			System.err.println("USAGE: java " + HBaseClientTester.class.getName() + " dataSourceName propertyFile <tableName> <columnFamilyName>");
			System.exit(1);
		}
		
		LOG.info("Starting ...");

		Properties conf = new Properties();
		
		conf.load(HBaseClientTester.class.getClassLoader().getResourceAsStream(args[1]));

		HashMap<String, String> prop = new HashMap<String, String>();
		for (Object key : conf.keySet()) {
			Object val = conf.get(key);
			prop.put((String) key, (String) val);
		}

		hc = new HBaseClient(args[0], prop);

		if (args.length == 3) {
			List<String> dbList = hc.getTableList(args[2]);
			if (dbList.size() == 0) {
				System.out.println("No tables found with db filter [" + args[2] + "]");
			} else {
				for (String str : dbList) {
					System.out.println("table: " + str);
				}
			}
		} else if (args.length == 4) {
			List<String> tableList = hc.getColumnFamilyList(args[2], args[3]);
			if (tableList.size() == 0) {
				System.out.println("No column families found under table [" + args[2] + "] with columnfamily filter [" + args[3] + "]");
			} else {
				for (String str : tableList) {
					System.out.println("ColumnFamily: " + str);
				}
			}
		}

	}

}
