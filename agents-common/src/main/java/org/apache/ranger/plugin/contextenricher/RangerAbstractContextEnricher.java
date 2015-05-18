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

package org.apache.ranger.plugin.contextenricher;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerContextEnricherDef;


public abstract class RangerAbstractContextEnricher implements RangerContextEnricher {
	private static final Log LOG = LogFactory.getLog(RangerAbstractContextEnricher.class);

	protected RangerContextEnricherDef enricherDef;
	protected String serviceName;
	protected RangerServiceDef serviceDef;
	protected String componentServiceName;
	protected RangerServiceDef componentServiceDef;

	private Map<String, String> options = null;

	@Override
	public void setContextEnricherDef(RangerContextEnricherDef enricherDef) {
		this.enricherDef = enricherDef;
	}
	
	@Override
	public void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAbstractContextEnricher.init(" + enricherDef + ")");
		}

		options = enricherDef.getEnricherOptions();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAbstractContextEnricher.init(" + enricherDef + ")");
		}
	}

	@Override
	public void setContextServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	@Override
	public void setContextServiceDef(RangerServiceDef serviceDef) {
		this.serviceDef = serviceDef;
	}

	@Override
	public void setContextComponentServiceName(String componentServiceName) {
		this.componentServiceName = componentServiceName;
	}

	@Override
	public void setContextComponentServiceDef(RangerServiceDef componentServiceDef) {
		this.componentServiceDef = componentServiceDef;
	}

	public String getOption(String name) {
		String ret = null;

		if(options != null && name != null) {
			ret = options.get(name);
		}

		return ret;
	}

	public String getOption(String name, String defaultValue) {
		String ret = getOption(name);

		if(StringUtils.isEmpty(ret)) {
			ret = defaultValue;
		}

		return ret;
	}

	public boolean getBooleanOption(String name) {
		String val = getOption(name);

		boolean ret = StringUtils.isEmpty(val) ? false : Boolean.parseBoolean(val);

		return ret;
	}

	public boolean getBooleanOption(String name, boolean defaultValue) {
		String strVal = getOption(name);

		boolean ret = StringUtils.isEmpty(strVal) ? defaultValue : Boolean.parseBoolean(strVal);

		return ret;
	}

	public char getCharOption(String name, char defaultValue) {
		String strVal = getOption(name);

		char ret = StringUtils.isEmpty(strVal) ? defaultValue : strVal.charAt(0);

		return ret;
	}

	public Properties readProperties(String fileName) {
		Properties ret = null;
		
		InputStream inStr = null;

		try {
			inStr = new FileInputStream(fileName);

			Properties prop = new Properties();

			prop.load(inStr);

			ret = prop;
		} catch(Exception excp) {
			LOG.error("failed to load properties from file '" + fileName + "'", excp);
		} finally {
			if(inStr != null) {
				try {
					inStr.close();
				} catch(Exception excp) {
					// ignore
				}
			}
		}

		return ret;
	}
}
