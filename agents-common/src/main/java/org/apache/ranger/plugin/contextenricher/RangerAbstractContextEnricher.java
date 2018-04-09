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
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerContextEnricherDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;


public abstract class RangerAbstractContextEnricher implements RangerContextEnricher {
	private static final Log LOG = LogFactory.getLog(RangerAbstractContextEnricher.class);

	protected RangerContextEnricherDef enricherDef;
	protected String serviceName;
	protected String appId;
	protected RangerServiceDef serviceDef;

	@Override
	public void setEnricherDef(RangerContextEnricherDef enricherDef) {
		this.enricherDef = enricherDef;
	}

	@Override
	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	@Override
	public void setServiceDef(RangerServiceDef serviceDef) {
		this.serviceDef = serviceDef;
	}

	@Override
	public void setAppId(String appId) {
		this.appId = appId;
	}

	@Override
	public void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAbstractContextEnricher.init(" + enricherDef + ")");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAbstractContextEnricher.init(" + enricherDef + ")");
		}
	}

	@Override
	public void enrich(RangerAccessRequest request, Object dataStore) {
		enrich(request);
	}

	@Override
	public boolean preCleanup() {
		return true;
	}

	@Override
	public void cleanup() {
	}

	@Override
	public String getName() {
		return enricherDef == null ? null : enricherDef.getName();
	}

	public RangerContextEnricherDef getEnricherDef() {
		return enricherDef;
	}

	public String getServiceName() {
		return serviceName;
	}

	public RangerServiceDef getServiceDef() {
		return serviceDef;
	}

	public String getAppId() {
		return appId;
	}

	public String getOption(String name) {
		String ret = null;

		Map<String, String> options = enricherDef != null ? enricherDef.getEnricherOptions() : null;

		if(options != null && name != null) {
			ret = options.get(name);
		}

		return ret;
	}

	public String getOption(String name, String defaultValue) {
		String ret = defaultValue;
		String val = getOption(name);

		if(val != null) {
			ret = val;
		}

		return ret;
	}

	public boolean getBooleanOption(String name, boolean defaultValue) {
		boolean ret = defaultValue;
		String  val = getOption(name);

		if(val != null) {
			ret = Boolean.parseBoolean(val);
		}

		return ret;
	}

	public char getCharOption(String name, char defaultValue) {
		char   ret = defaultValue;
		String val = getOption(name);

		if(! StringUtils.isEmpty(val)) {
			ret = val.charAt(0);
		}

		return ret;
	}

	public long getLongOption(String name, long defaultValue) {
		long ret = defaultValue;
		String  val = getOption(name);

		if(val != null) {
			ret = Long.parseLong(val);
		}

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
