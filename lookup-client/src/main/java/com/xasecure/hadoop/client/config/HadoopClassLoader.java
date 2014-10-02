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

 package com.xasecure.hadoop.client.config;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.hadoop.client.exceptions.HadoopException;

public class HadoopClassLoader extends ClassLoader {
	
	private static final Log LOG = LogFactory.getLog(HadoopClassLoader.class) ;
	
	private HadoopConfigHolder confHolder ;
	
	public HadoopClassLoader(HadoopConfigHolder confHolder) {
		super(Thread.currentThread().getContextClassLoader()) ;
		this.confHolder = confHolder;
	}
	
	
	@Override
	protected URL findResource(String resourceName) {
		LOG.debug("findResource(" + resourceName + ") is called.") ;
		URL ret = null;
	
		if (confHolder.hasResourceExists(resourceName)) {
			ret = buildResourceFile(resourceName) ;
		}
		else {
			ret = super.findResource(resourceName);
		}
		LOG.debug("findResource(" + resourceName + ") is returning [" + ret + "]") ;
		return ret ;
	}
	
	
	@SuppressWarnings("deprecation")
	private URL buildResourceFile(String aResourceName) {
		URL ret = null ;
		String prefix = aResourceName ;
		String suffix = ".txt" ;

		Properties prop = confHolder.getProperties(aResourceName) ;
		LOG.debug("Building XML for: " + prop.toString());
		if (prop != null && prop.size() > 0) {
			if (aResourceName.contains(".")) {
				int lastDotFound = aResourceName.indexOf(".") ;
				prefix = aResourceName.substring(0,lastDotFound) + "-" ;
				suffix = aResourceName.substring(lastDotFound) ;
			}
			
			try {
				File tempFile = File.createTempFile(prefix, suffix) ;
				tempFile.deleteOnExit();
				PrintWriter out = new PrintWriter(new FileWriter(tempFile)) ;
				out.println("<?xml version=\"1.0\"?>") ;
				out.println("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>") ;
				out.println("<configuration xmlns:xi=\"http://www.w3.org/2001/XInclude\">") ;
				for(Object keyobj : prop.keySet()) {
					String key = (String)keyobj;
					String val = prop.getProperty(key) ;
					out.println("<property><name>" + key.trim() + "</name><value>" + val + "</value></property>") ;
				}
				out.println("</configuration>") ;
				out.close() ;
				ret = tempFile.toURL() ;
			} catch (IOException e) {
				throw new HadoopException("Unable to load create hadoop configuration file [" + aResourceName + "]", e) ;
			}
			
		}
		
		return ret ;

	}
	

}
