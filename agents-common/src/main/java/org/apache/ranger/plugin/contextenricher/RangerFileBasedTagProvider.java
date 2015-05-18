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

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerResource;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;


public class RangerFileBasedTagProvider extends RangerAbstractContextEnricher {
	private static final Log LOG = LogFactory.getLog(RangerFileBasedTagProvider.class);

	private Properties resourceTagsMap = null;
	String dataFile = null;
	private Gson gsonBuilder = null;
	
	@Override
	public void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerFileBasedTagProvider.init()");
		}
		
		super.init();

		dataFile = getOption("dataFile", "/etc/ranger/data/resourceTags.txt");

		resourceTagsMap = readProperties(dataFile);

		gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z")
				.setPrettyPrinting()
				.create();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerFileBasedTagProvider.init()");
		}
	}

	@Override
	public void enrich(RangerAccessRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerFileBasedTagProvider.enrich(" + request + ")");
		}
		
		if(request != null && resourceTagsMap != null) {
			Map<String, Object> context = request.getContext();
			/*
			This needs to know about :
				- componentServiceDef (to filter on component-type which is required for getting matchers), and
				- serviceName (to filter on cluster-specific tags)
			*/
			// Provider is file-based.
			// tags are a JSON strings

			String requestedResource = request.getResource().getAsString(componentServiceDef);

			if(LOG.isDebugEnabled()) {
				LOG.debug("RangerFileBasedTagProvider.enrich(): requestedResource = '"+ requestedResource +"'");
			}
			String tagsJsonString = resourceTagsMap.getProperty(requestedResource);

			if(!StringUtils.isEmpty(tagsJsonString) && context != null) {
				try {
					Type listType = new TypeToken<List<RangerResource.RangerResourceTag>>() {
					}.getType();
					List<RangerResource.RangerResourceTag> tagList = gsonBuilder.fromJson(tagsJsonString, listType);

					context.put(RangerPolicyEngine.KEY_CONTEXT_TAGS, tagList);
				} catch (Exception e) {
					LOG.error("RangerFileBasedTagProvider.enrich(): error parsing file " + this.dataFile + "exception=" + e);
				}
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("RangerFileBasedTagProvider.enrich(): skipping due to unavailable context or tags. context=" + context + "; tags=" + tagsJsonString);
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerFileBasedTagProvider.enrich(" + request + ")");
		}
	}
}
