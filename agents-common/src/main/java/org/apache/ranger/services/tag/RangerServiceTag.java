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

package org.apache.ranger.services.tag;

import java.util.*;

import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RangerServiceTag extends RangerBaseService {

	private static final Log LOG = LogFactory.getLog(RangerServiceTag.class);

	public static final String TAG	= "tag";

	public static final String propertyPrefix = "ranger.plugin.tag";

	public static final String applicationId = "Ranger-GUI";

	public RangerServiceTag() {
		super();
	}

	@Override
	public void init(RangerServiceDef serviceDef, RangerService service) {
		super.init(serviceDef, service);
	}

	@Override
	public HashMap<String,Object> validateConfig() throws Exception {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		String 	serviceName         = getServiceName();
		boolean connectivityStatus  = false;
		String message              = null;

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceTag.validateConfig -  Service: (" + serviceName + " )");
		}

		RangerAdminClient adminClient = createAdminClient(serviceName);

		try {
			adminClient.getTagTypes(".*");
			connectivityStatus = true;
		} catch (Exception e) {
			LOG.error("RangerServiceTag.validateConfig() Error:" + e);
			connectivityStatus = false;
			message = "Cannot connect to TagResource Repository, Exception={" + e + "}. " + "Please check "
					+ propertyPrefix + " sub-properties.";
		}

		ret.put("connectivityStatus", connectivityStatus);
		ret.put("message", message);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceTag.validateConfig - Response : (" + ret + " )");
		}

		return ret;
	}

	@Override
	public List<String> lookupResource(ResourceLookupContext context) throws Exception {
		String 	serviceName  	   = getServiceName();

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceTag.lookupResource -  Context: (" + context + ")");
		}

		List<String> tagTypeList = new ArrayList<>();

		if (context != null) {

			String userInput = context.getUserInput();
			String resource = context.getResourceName();
			Map<String, List<String>> resourceMap = context.getResources();
			final List<String> userProvidedTagList = new ArrayList<>();

			if (resource != null && resourceMap != null && resourceMap.get(TAG) != null) {

				for (String tag : resourceMap.get(TAG)) {
					userProvidedTagList.add(tag);
				}

				String suffix = ".*";
				String pattern;

				if (userInput == null) {
					pattern = suffix;
				} else {
					pattern = userInput + suffix;
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("RangerServiceTag.lookupResource -  pattern : (" + pattern + ")");
				}

				try {

					RangerAdminClient adminClient = createAdminClient(serviceName);

					tagTypeList = adminClient.getTagTypes(pattern);

					tagTypeList.removeAll(userProvidedTagList);

				} catch (Exception e) {
					LOG.error("RangerServiceTag.lookupResource -  Exception={" + e + "}. " + "Please check " +
							propertyPrefix + " sub-properties.");
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceTag.lookupResource()");
		}

		return tagTypeList;
	}

	public static RangerAdminClient createAdminClient( String tagServiceName ) {
		return RangerBasePlugin.createAdminClient(tagServiceName, applicationId, propertyPrefix);
	}

}
