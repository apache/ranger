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

package org.apache.ranger.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class RangerProperties extends  HashMap<String,String>  {
	
	private static final long serialVersionUID = -4094378755892810987L;

	private final Logger LOG = Logger.getLogger(RangerProperties.class) ;

	private final String XMLCONFIG_FILENAME_DELIMITOR = ",";
	private final String XMLCONFIG_PROPERTY_TAGNAME = "property" ;
	private final String XMLCONFIG_NAME_TAGNAME = "name" ;
	private final String XMLCONFIG_VALUE_TAGNAME = "value" ;

	private String xmlConfigFileNames = null;

	public RangerProperties(String xmlConfigFileNames) {
		this.xmlConfigFileNames = xmlConfigFileNames;
		initProperties();
	}

	private void initProperties() {
		
		if (xmlConfigFileNames == null || xmlConfigFileNames.isEmpty())
			return;

		String[] fnList = xmlConfigFileNames
				.split(XMLCONFIG_FILENAME_DELIMITOR);

		for (String fn : fnList) {
			try {
				loadXMLConfig(fn) ;
			}
			catch(IOException ioe) {
				LOG.error("Unable to load configuration from file: [" + fn + "]", ioe);
			}
		}

	}

	private void loadXMLConfig(String fileName) throws IOException {

		try {
			InputStream in = getFileInputStream(fileName);

			if (in == null) {
				return;
			}

			DocumentBuilderFactory xmlDocumentBuilderFactory = DocumentBuilderFactory
					.newInstance();
			xmlDocumentBuilderFactory.setIgnoringComments(true);
			xmlDocumentBuilderFactory.setNamespaceAware(true);
			DocumentBuilder xmlDocumentBuilder = xmlDocumentBuilderFactory
					.newDocumentBuilder();
			Document xmlDocument = xmlDocumentBuilder.parse(in);
			xmlDocument.getDocumentElement().normalize();

			NodeList nList = xmlDocument.getElementsByTagName(XMLCONFIG_PROPERTY_TAGNAME);

			for (int temp = 0; temp < nList.getLength(); temp++) {

				Node nNode = nList.item(temp);

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					Element eElement = (Element) nNode;

					String propertyName = "";
					String propertyValue = "";
					
					if (eElement.getElementsByTagName(XMLCONFIG_NAME_TAGNAME).item(0) != null) {
						propertyName = eElement.getElementsByTagName(XMLCONFIG_NAME_TAGNAME).item(0).getTextContent().trim();
					}
					
					if (eElement.getElementsByTagName(XMLCONFIG_VALUE_TAGNAME).item(0) != null) {
						propertyValue = eElement.getElementsByTagName(XMLCONFIG_VALUE_TAGNAME).item(0).getTextContent().trim();
					}
					
					if (get(propertyName) != null) 
						remove(propertyName) ;
					
					if (propertyValue != null)
						put(propertyName, propertyValue);
					
				}
			}
		} catch (Throwable t) {
			throw new IOException(t);
		}
	}

	private InputStream getFileInputStream(String path)
			throws FileNotFoundException {

		InputStream ret = null;

		File f = new File(path);

		if (f.exists()) {
			ret = new FileInputStream(f);
		} else {
			ret = getClass().getResourceAsStream(path);

			if (ret == null) {
				if (!path.startsWith("/")) {
					ret = getClass().getResourceAsStream("/" + path);
				}
			}

			if (ret == null) {
				ret = ClassLoader.getSystemClassLoader().getResourceAsStream(
						path);
				if (ret == null) {
					if (!path.startsWith("/")) {
						ret = ClassLoader.getSystemResourceAsStream("/" + path);
					}
				}
			}
		}

		return ret;
	}
	
	
}
