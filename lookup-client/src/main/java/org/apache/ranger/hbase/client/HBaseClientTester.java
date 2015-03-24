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

 package org.apache.ranger.hbase.client;

import java.io.IOException;
import java.io.InputStream;
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
		InputStream in = HBaseClientTester.class.getClassLoader().getResourceAsStream(args[1]) ;
		try {
		conf.load(in);
		}
		finally {
			if (in != null) {
				try {
				in.close();
				}
				catch(IOException ioe) {
					// Ignore IOE when closing stream
				}
			}
		}

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
