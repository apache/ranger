package com.xasecure.hive.client;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class HiveClientTester  {

	public static void main(String[] args) throws Throwable {
		
		HiveClient hc = null ;
		
		if (args.length <= 2) {
			System.err.println("USAGE: java " + HiveClientTester.class.getName() + " dataSourceName propertyFile <databaseName> <tableName> <columnName>") ;
			System.exit(1) ;
		}
		
		
		try {
			
			Properties conf = new Properties() ;
			conf.load(HiveClientTester.class.getClassLoader().getResourceAsStream(args[1]));
			
			HashMap<String,String> prop = new HashMap<String,String>() ;
			for(Object key : conf.keySet()) {
				Object val = conf.get(key) ;
				prop.put((String)key, (String)val) ;
			}

			
			hc = new HiveClient(args[0], prop) ;
			
			
			if (args.length == 3) {
				List<String> dbList = hc.getDatabaseList(args[2]) ;
				if (dbList.size() == 0) {
					System.out.println("No database found with db filter [" + args[2] + "]") ;
				}
				else {
					for (String str : dbList ) {
						System.out.println("database: " + str ) ;
					}
				}
			}
			else if (args.length == 4) {
				List<String> tableList = hc.getTableList(args[2], args[3]) ;
				if (tableList.size() == 0) {
					System.out.println("No tables found under database[" + args[2] + "] with table filter [" + args[3] + "]") ;
				}
				else {
					for(String str : tableList) {
						System.out.println("Table: " + str) ;
					}
				}
			}
			else if (args.length == 5) {
				List<String> columnList = hc.getColumnList(args[2], args[3], args[4]) ;
				if (columnList.size() == 0) {
					System.out.println("No columns found for db:" + args[2] + ", table: [" + args[3] + "], with column filter [" + args[4] + "]") ;
				}
				else {
					for (String str : columnList ) {
						System.out.println("Column: " + str) ;
					}
				}
			}
			
		}
		finally {
			if (hc != null) {
				hc.close();
			}
		}
		
	}
	

}
