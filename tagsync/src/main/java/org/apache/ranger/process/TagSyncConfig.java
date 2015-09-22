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

package org.apache.ranger.process;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.URL;
import java.util.Properties;

/**
 * Created by akulkarni on 9/11/15.
 */
public class TagSyncConfig {
	private static final Logger LOG = Logger.getLogger(TagSyncConfig.class) ;

	public static final String CONFIG_FILE = "ranger-tagsync-site.xml";

	public static final String DEFAULT_CONFIG_FILE = "ranger-tagsync-default-site.xml";

	public static final String TAGSYNC_ENABLED_PROP = "ranger.tagsync.enabled" ;

	public static final String TAGSYNC_PORT_PROP = "ranger.tagsync.port" ;

	public static final String TAGSYNC_SSL_PROP = "ranger.tagsync.ssl" ;

	public static final String TAGSYNC_LOGDIR_PROP = "ranger.tagsync.logdir" ;

	private static final String TAGSYNC_PM_URL_PROP = 	"ranger.tagsync.policymanager.baseURL";

	private static final String TAGSYNC_PM_SSL_CONFIG_FILE_PROP = "ranger.tagsync.policymanager.ssl.config.file";

	private static final String TAGSYNC_PM_SSL_BASICAUTH_USERNAME_PROP = "ranger.tagsync.policymanager.basicauth.username";

	private static final String TAGSYNC_PM_SSL_BASICAUTH_PASSWORD_PROP = "ranger.tagsync.policymanager.basicauth.password";

	private static final String TAGSYNC_SOURCE_FILE_PROP = "ranger.tagsync.source.file";

	private static final String TAGSYNC_SLEEP_TIME_IN_MILLIS_BETWEEN_CYCLE_PROP = "ranger.tagsync.sleeptimeinmillisbetweensynccycle";

	private static final String TAGSYNC_SOURCE_CLASS_PROP = "ranger.tagsync.source.impl.class";

	private static final String TAGSYNC_SINK_CLASS_PROP = "ranger.tagsync.sink.impl.class";

	private static final String TAGSYNC_SOURCE_ATLAS_PROP = "atlas.endpoint";

	private static volatile TagSyncConfig instance = null;

	private Properties prop = new Properties() ;

	public static TagSyncConfig getInstance() {
	/*
		TagSyncConfig ret = instance;
		if (ret == null) {
			synchronized(TagSyncConfig.class) {
				if (ret == null) {
					ret = instance = new TagSyncConfig();
					LOG.debug("TagSyncConfig = {" + ret + "}");
				}
			}
		}
	*/
		TagSyncConfig newConfig = new TagSyncConfig();
		newConfig.init();
		return newConfig;
	}

	public Properties getProperties() {
		return prop;
	}

	public static InputStream getFileInputStream(String path) throws FileNotFoundException {

		InputStream ret = null;

		File f = new File(path);

		if (f.exists() && f.isFile() && f.canRead()) {
			ret = new FileInputStream(f);
		} else {
			ret = TagSyncConfig.class.getResourceAsStream(path);

			if (ret == null) {
				if (! path.startsWith("/")) {
					ret = TagSyncConfig.class.getResourceAsStream("/" + path);
				}
			}

			if (ret == null) {
				ret = ClassLoader.getSystemClassLoader().getResourceAsStream(path) ;
				if (ret == null) {
					if (! path.startsWith("/")) {
						ret = ClassLoader.getSystemResourceAsStream("/" + path);
					}
				}
			}
		}

		return ret;
	}

	public static String getResourceFileName(String path) {

		String ret = null;

		if (StringUtils.isNotBlank(path)) {

			File f = new File(path);

			if (f.exists() && f.isFile() && f.canRead()) {
				ret = path;
			} else {

				URL fileURL = TagSyncConfig.class.getResource(path);
				if (fileURL == null) {
					if (!path.startsWith("/")) {
						fileURL = TagSyncConfig.class.getResource("/" + path);
					}
				}

				if (fileURL == null) {
					fileURL = ClassLoader.getSystemClassLoader().getResource(path);
					if (fileURL == null) {
						if (!path.startsWith("/")) {
							fileURL = ClassLoader.getSystemClassLoader().getResource("/" + path);
						}
					}
				}

				if (fileURL != null) {
					try {
						ret = fileURL.getFile();
					} catch (Exception exception) {
						LOG.error(path + " is not a file", exception);
					}
				} else {
					LOG.error("URL not found for " + path + " or no privilege for reading file " + path);
				}
			}
		}

		return ret;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();

		sb.append("CONFIG_FILE=").append(CONFIG_FILE).append(", ")
				.append("DEFAULT_CONFIG_FILE=").append(DEFAULT_CONFIG_FILE).append("\n");

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		PrintStream printStream = new PrintStream(outputStream);
		prop.list(printStream);
		printStream.close();
		sb.append(outputStream.toString());

		return sb.toString();
	}

	static public boolean isTagSyncEnabled(Properties prop) {
		String val = prop.getProperty(TAGSYNC_ENABLED_PROP);
		return !(val != null && val.trim().equalsIgnoreCase("falae"));
	}

	static public String getTagSyncPort(Properties prop) {
		String val = prop.getProperty(TAGSYNC_PORT_PROP);
		return val;
	}

	static public boolean isTagSyncSsl(Properties prop) {
		String val = prop.getProperty(TAGSYNC_SSL_PROP);
		return (val != null && val.trim().equalsIgnoreCase("true"));
	}

	static public String getTagSyncLogdir(Properties prop) {
		String val = prop.getProperty(TAGSYNC_LOGDIR_PROP);
		return val;
	}

	static public long getSleepTimeInMillisBetweenCycle(Properties prop) {
		String val = prop.getProperty(TAGSYNC_SLEEP_TIME_IN_MILLIS_BETWEEN_CYCLE_PROP);
		return Long.valueOf(val);
	}

	static public String getTagSourceClassName(Properties prop) {
		String val = prop.getProperty(TAGSYNC_SOURCE_CLASS_PROP);
		return val;
	}

	static public String getTagSinkClassName(Properties prop) {
		String val = prop.getProperty(TAGSYNC_SINK_CLASS_PROP);
		return val;
	}

	static public String getPolicyMgrUrl(Properties prop) {
		String val = prop.getProperty(TAGSYNC_PM_URL_PROP);
		return val;
	}

	static public String getPolicyMgrSslConfigFile(Properties prop) {
		String val = prop.getProperty(TAGSYNC_PM_SSL_CONFIG_FILE_PROP);
		return val;
	}

	static public String getPolicyMgrUserName(Properties prop) {
		String val = prop.getProperty(TAGSYNC_PM_SSL_BASICAUTH_USERNAME_PROP);
		return val;
	}

	static public String getPolicyMgrPassword(Properties prop) {
		String val = prop.getProperty(TAGSYNC_PM_SSL_BASICAUTH_PASSWORD_PROP);
		return val;
	}

	static public String getTagSourceFileName(Properties prop) {
		String val = prop.getProperty(TAGSYNC_SOURCE_FILE_PROP);
		return val;
	}

	static public String getAtlasEndpoint(Properties prop) {
		String val = prop.getProperty(TAGSYNC_SOURCE_ATLAS_PROP);
		return val;
	}

	static public String getAtlasSslConfigFileName(Properties prop) {
		return "";
	}

	private TagSyncConfig() {
		init() ;
	}

	private void init() {
		readConfigFile(CONFIG_FILE);
		readConfigFile(DEFAULT_CONFIG_FILE);
	}

	private void readConfigFile(String fileName) {
		try {
			InputStream in = getFileInputStream(fileName);
			if (in != null) {
				try {
					DocumentBuilderFactory xmlDocumentBuilderFactory = DocumentBuilderFactory
							.newInstance();
					xmlDocumentBuilderFactory.setIgnoringComments(true);
					xmlDocumentBuilderFactory.setNamespaceAware(true);
					DocumentBuilder xmlDocumentBuilder = xmlDocumentBuilderFactory
							.newDocumentBuilder();
					Document xmlDocument = xmlDocumentBuilder.parse(in);
					xmlDocument.getDocumentElement().normalize();

					NodeList nList = xmlDocument
							.getElementsByTagName("property");

					for (int temp = 0; temp < nList.getLength(); temp++) {

						Node nNode = nList.item(temp);

						if (nNode.getNodeType() == Node.ELEMENT_NODE) {

							Element eElement = (Element) nNode;

							String propertyName = "";
							String propertyValue = "";
							if (eElement.getElementsByTagName("name").item(
									0) != null) {
								propertyName = eElement
										.getElementsByTagName("name")
										.item(0).getTextContent().trim();
							}
							if (eElement.getElementsByTagName("value")
									.item(0) != null) {
								propertyValue = eElement
										.getElementsByTagName("value")
										.item(0).getTextContent().trim();
							}

							if (prop.get(propertyName) != null) {
								prop.remove(propertyName) ;
							}

							prop.put(propertyName, propertyValue);

						}
					}
				}
				finally {
					try {
						in.close() ;
					}
					catch(IOException ioe) {
						// Ignore IOE when closing stream
					}
				}
			}
		} catch (Throwable e) {
			throw new RuntimeException("Unable to load configuration file [" + CONFIG_FILE + "]", e) ;
		}
	}

}
