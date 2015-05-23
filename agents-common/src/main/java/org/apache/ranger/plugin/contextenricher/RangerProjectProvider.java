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

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;


public class RangerProjectProvider extends RangerAbstractContextEnricher {
	private static final Log LOG = LogFactory.getLog(RangerProjectProvider.class);

	private String     contextName    = "PROJECT";
	private Properties userProjectMap = null;
	
	@Override
	public void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerProjectProvider.init(" + enricherDef + ")");
		}
		
		super.init();
		
		contextName = getOption("contextName", "PROJECT");

		String dataFile = getOption("dataFile", "/etc/ranger/data/userProject.txt");

		userProjectMap = readProperties(dataFile);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerProjectProvider.init(" + enricherDef + ")");
		}
	}

	@Override
	public void enrich(RangerAccessRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerProjectProvider.enrich(" + request + ")");
		}
		
		if(request != null && userProjectMap != null) {
			Map<String, Object> context = request.getContext();
			String              project = userProjectMap.getProperty(request.getUser());
	
			if(context != null && !StringUtils.isEmpty(project)) {
				request.getContext().put(contextName, project);
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("RangerProjectProvider.enrich(): skipping due to unavailable context or project. context=" + context + "; project=" + project);
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerProjectProvider.enrich(" + request + ")");
		}
	}
}
