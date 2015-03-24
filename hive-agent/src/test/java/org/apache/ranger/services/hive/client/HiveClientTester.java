/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.services.hive.client;
 

import java.io.IOException;
import java.io.InputStream;
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
			
			InputStream in = HiveClientTester.class.getClassLoader().getResourceAsStream(args[1]) ;
			try {
				conf.load(in);
			}
			finally {
				if (in != null) {
					try {
						in.close();
					}
					catch(IOException ioe) {
						// Ignore IOException when closing the stream
					}
				}
			}
			
			HashMap<String,String> prop = new HashMap<String,String>() ;
			for(Object key : conf.keySet()) {
				Object val = conf.get(key) ;
				prop.put((String)key, (String)val) ;
			}

			
			hc = new HiveClient(args[0], prop) ;
			
			
			if (args.length == 3) {
				List<String> dbList = hc.getDatabaseList(args[2],null) ;
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
				List<String> tableList = hc.getTableList(args[3], null, null) ;
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
				List<String> columnList = hc.getColumnList(args[4], null, null, null) ;
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
