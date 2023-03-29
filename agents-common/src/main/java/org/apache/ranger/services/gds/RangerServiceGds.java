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

package org.apache.ranger.services.gds;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.store.GdsStore;
import org.apache.ranger.plugin.store.PList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RangerServiceGds extends RangerBaseService {
	private static final Logger LOG = LoggerFactory.getLogger(RangerServiceGds.class);

	public static final String RESOURCE_NAME_DATASET = "dataset";
	public static final String RESOURCE_NAME_PROJECT = "project";

	private GdsStore gdsStore;

	public RangerServiceGds() {
		super();
	}

	@Override
	public void init(RangerServiceDef serviceDef, RangerService service) {
		super.init(serviceDef, service);
	}

	public void setGdsStore(GdsStore gdsStore) {
		this.gdsStore = gdsStore;
	}

	@Override
	public Map<String,Object> validateConfig() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceGds.validateConfig(" + serviceName + " )");
		}

		Map<String, Object> ret = new HashMap<>();

		ret.put("connectivityStatus", true);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceGds.validateConfig(" + serviceName + " ): " + ret);
		}

		return ret;
	}

	@Override
	public List<String> lookupResource(ResourceLookupContext context) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceGds.lookupResource(" + context + ")");
		}

		List<String> ret             = new ArrayList<>();
		String       resourceType    = context != null ? context.getResourceName() : null;
		List<String> valuesToExclude = null;
		List<String> resourceNames   = null;

		if (StringUtils.equals(resourceType, RESOURCE_NAME_DATASET)) {
			PList<String> datasets = gdsStore != null ? gdsStore.getDatasetNames(null) : null;

			resourceNames   = datasets != null ? datasets.getList() : null;
			valuesToExclude = context.getResources() != null ? context.getResources().get(RESOURCE_NAME_DATASET) : null;
		} else if (StringUtils.equals(resourceType, RESOURCE_NAME_PROJECT)) {
			PList<String> projects = gdsStore != null ? gdsStore.getProjectNames(null) : null;

			resourceNames   = projects != null ? projects.getList() : null;
			valuesToExclude = context.getResources() != null ? context.getResources().get(RESOURCE_NAME_PROJECT) : null;
		}

		if (resourceNames != null) {
			if (valuesToExclude != null) {
				resourceNames.removeAll(valuesToExclude);
			}

			String valueToMatch = context.getUserInput();

			if (StringUtils.isNotEmpty(valueToMatch)) {
				if (!valueToMatch.endsWith("*")) {
					valueToMatch += "*";
				}

				for (String resourceName : resourceNames) {
					if (FilenameUtils.wildcardMatch(resourceName, valueToMatch)) {
						ret.add(resourceName);
					}
				}
			}
		}


		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceGds.lookupResource(): {} count={}", resourceType, ret.size());
		}

		return ret;
	}
}
