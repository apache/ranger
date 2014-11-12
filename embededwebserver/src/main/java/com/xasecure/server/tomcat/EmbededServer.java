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

 package com.xasecure.server.tomcat;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.logging.Logger;

import javax.servlet.ServletException;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.valves.AccessLogValve;

public class EmbededServer {
	
	private static final Logger LOG = Logger.getLogger(EmbededServer.class.getName()) ;
	
	private static final String DEFAULT_CONFIG_FILENAME = "ranger_webserver.properties" ;
	
	private static String configFile = DEFAULT_CONFIG_FILENAME ;
	
	private Properties serverConfigProperties = new Properties() ;

	public static void main(String[] args) {
		new EmbededServer(args).start() ;
	}
	
	
	public EmbededServer(String[] args) {
		if (args.length > 0) {
			configFile = args[0] ;
		}
		initConfig() ;
	}
	
	
	private void initConfig() {
		
		String cfgFile =  getResourceFileName(configFile) ;
		
		serverConfigProperties.clear() ;
		
		InputStream in = null ;
		try {
			
			in = new FileInputStream(cfgFile) ;
			serverConfigProperties.load(in);
		}
		catch(FileNotFoundException fnf) {
			LOG.severe("Unable to find config  file [" + cfgFile + "]");
			fnf.printStackTrace(); 
		}
		catch(IOException ioe) {
			LOG.severe("Unable to load config  file [" + cfgFile + "]");
			ioe.printStackTrace(); 
		}
		serverConfigProperties.list(System.out);
	}
	
	public static int DEFAULT_SHUTDOWN_PORT = 6185 ;
	public static String DEFAULT_SHUTDOWN_COMMAND = "SHUTDOWN" ;
	
	
	public void start() {
		Tomcat server = new Tomcat();
		
		String hostName = getConfig("service.host") ;
		int serverPort = getIntConfig("http.service.port", 6181) ;
		int sslPort = getIntConfig("https.service.port",-1) ;
		int shutdownPort = getIntConfig("service.shutdownPort", DEFAULT_SHUTDOWN_PORT ) ;
		String shutdownCommand = getConfig("service.shutdownCommand", DEFAULT_SHUTDOWN_COMMAND ) ;
		
		server.setHostname(hostName);
		server.setPort(serverPort);
		server.getServer().setPort(shutdownPort);
		server.getServer().setShutdown(shutdownCommand);
		
		if (sslPort > 0) {
			Connector ssl = new Connector() ;
			ssl.setPort(sslPort) ;
			ssl.setSecure(true);
			ssl.setScheme("https") ;
			ssl.setAttribute("SSLEnabled", getConfig("https.attrib.SSLEnabled", "true"));
			ssl.setAttribute("sslProtocol", getConfig("https.attrib.sslProtocol", "TLS")) ;
			ssl.setAttribute("clientAuth", getConfig("https.attrib.clientAuth", "false"));
			ssl.setAttribute("keyAlias", getConfig("https.attrib.keyAlias") ) ;
			ssl.setAttribute("keystorePass", getConfig("https.attrib.keystorePass"));
			ssl.setAttribute("keystoreFile",  getConfig("https.attrib.keystoreFile")) ;
			
			String enabledProtocols = "SSLv2Hello, TLSv1, TLSv1.1, TLSv1.2" ;
			ssl.setAttribute("sslEnabledProtocols", enabledProtocols ) ;
			
			server.getService().addConnector(ssl); 
		}

		
		File baseDir = new File(".") ;
		
		File logDirectory = new File(baseDir, "logs") ;
		if (! logDirectory.exists()) {
			logDirectory.mkdirs() ;
		}
		
		AccessLogValve valve = new AccessLogValve() ;
		valve.setRotatable(true) ;
		valve.setAsyncSupported(true);
		valve.setBuffered(false);
		valve.setEnabled(true);
		valve.setFileDateFormat(getConfig("accesslog.dateformat","yyyy-MM-dd.HH")) ;
		valve.setDirectory(logDirectory.getAbsolutePath());
		valve.setRotatable(true);
		valve.setSuffix(".log");
		
		String logPattern = getConfig("accesslog.pattern", "%h %l %u %t \"%r\" %s %b") ;
		valve.setPattern(logPattern);	
				
		server.getHost().getPipeline().addValve(valve);
		
		try {
			String webapp_dir= getConfig("xa.webapp.dir");
			if( webapp_dir == null || webapp_dir.trim().isEmpty()) {
				//If webapp location property is not set, then let's dervice from catalina_base
				String catalina_base = getConfig("catalina.base");
				if( catalina_base == null || catalina_base.trim().isEmpty()) {
					LOG.severe("Tomcat Server failed to start: catalina.base and/or xa.webapp.dir is not set") ;
					System.exit(1);
				}
				webapp_dir = catalina_base + File.separator + "webapp";
				LOG.info("Deriving webapp folder from catalina.base property. folder=" + webapp_dir);
			}
			LOG.info("Webapp folder=" + webapp_dir);
			Context webappCtx = server.addWebapp("/",  new File(webapp_dir).getAbsolutePath()) ;
			webappCtx.init() ;
		} catch (ServletException e1) {
			LOG.severe("Tomcat Server failed to add webapp:" + e1.toString()) ;
			e1.printStackTrace();
		} catch(LifecycleException lce) {
			LOG.severe("Tomcat Server failed to start webapp:" + lce.toString()) ;
			lce.printStackTrace();
		}
				
		try {
			server.start(); 
			server.getServer().await();
		} catch (LifecycleException e) {
			LOG.severe("Tomcat Server failed to start:" + e.toString()) ;
			e.printStackTrace(); 
		} 
	}
	
	
	protected String getConfig(String key) {
		String value = serverConfigProperties.getProperty(key) ;
		if ( value == null || value.trim().isEmpty()) {
			//Value not found in properties file, let's try to get from System's property
			value = System.getProperty(key);
		}
		return value;
	}
	
	protected String getConfig(String key, String defaultValue) {
		String ret = getConfig(key) ;
		if (key == null) {
			ret = defaultValue ;
		}
		return ret;
	}
	
	protected int getIntConfig(String key, int defaultValue) {
		int ret = 0 ;
		String retStr = getConfig(key) ;
		if (retStr == null) {
			ret = defaultValue ;
		}
		else {
			ret = Integer.parseInt(retStr) ;
		}
		return ret;
	}
	
	private String getResourceFileName(String aResourceName) {
		
		String ret = aResourceName ;
		
		ClassLoader cl = getClass().getClassLoader() ;
		
		for (String path : new String[] { aResourceName, "/" + aResourceName }) {
			
			try {
				URL lurl = cl.getResource(path) ;
		
				if (lurl != null) {
					ret = lurl.getFile() ;
				}
			}
			catch(Throwable t) {
				ret = null;
			}
			if (ret != null) {
				break ;
			}

		}
		
		if (ret == null) {
			ret = aResourceName ;
		}
		
		return ret ;
		
		
		
	}

}
