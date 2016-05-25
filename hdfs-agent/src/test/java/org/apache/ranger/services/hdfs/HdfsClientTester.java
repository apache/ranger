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

package org.apache.ranger.services.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.ranger.services.hdfs.client.HdfsClient;

public class HdfsClientTester {

	public static void main(String[] args) throws Throwable {
		if (args.length < 3) {
			System.err.println("USAGE: java " + HdfsClient.class.getName() + " repositoryName propertyFile basedirectory  [filenameToMatch]") ;
			System.exit(1) ;
		}
		
		String repositoryName = args[0] ;
		String propFile = args[1] ;
		String baseDir = args[2] ;
		String fileNameToMatch = (args.length == 3 ? null : args[3]) ;

		Properties conf = new Properties() ;
		
		InputStream in = HdfsClientTester.class.getClassLoader().getResourceAsStream(propFile) ;
		try {
			conf.load(in);
		}
		finally {
			if (in != null) {
				try {
				in.close() ;
				}
				catch(IOException ioe) {
					// Ignore IOException created during close
				}
			}
		}
		
		HashMap<String,String> prop = new HashMap<String,String>() ;
		for(Object key : conf.keySet()) {
			Object val = conf.get(key) ;
			prop.put((String)key, (String)val) ;
		}
		
		HdfsClient fs = new HdfsClient(repositoryName, prop) ;
		List<String> fsList = fs.listFiles(baseDir, fileNameToMatch,null) ;
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
