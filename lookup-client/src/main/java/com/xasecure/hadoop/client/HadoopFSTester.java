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

 package com.xasecure.hadoop.client;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class HadoopFSTester {

	public static void main(String[] args) throws Throwable {
		if (args.length < 3) {
			System.err.println("USAGE: java " + HadoopFS.class.getName() + " repositoryName propertyFile basedirectory  [filenameToMatch]") ;
			System.exit(1) ;
		}
		
		String repositoryName = args[0] ;
		String propFile = args[1] ;
		String baseDir = args[2] ;
		String fileNameToMatch = (args.length == 3 ? null : args[3]) ;

		Properties conf = new Properties() ;
		conf.load(HadoopFSTester.class.getClassLoader().getResourceAsStream(propFile));
		
		HashMap<String,String> prop = new HashMap<String,String>() ;
		for(Object key : conf.keySet()) {
			Object val = conf.get(key) ;
			prop.put((String)key, (String)val) ;
		}
		
		HadoopFS fs = new HadoopFS(repositoryName, prop) ;
		List<String> fsList = fs.listFiles(baseDir, fileNameToMatch) ;
		if (fsList != null && fsList.size() > 0) {
			for(String s : fsList) {
				System.out.println(s) ;
			}
		}
		else {
			System.err.println("Unable to get file listing for [" + baseDir + (baseDir.endsWith("/") ? "" : "/") + fileNameToMatch + "]  in repository [" + repositoryName + "]") ;
		}

	}

}
