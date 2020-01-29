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
package org.apache.ranger.services.nifi.registry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.nifi.registry.client.NiFiRegistryClient;
import org.apache.ranger.services.nifi.registry.client.NiFiRegistryConnectionMgr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * RangerService for Apache NiFi Registry.
 */
public class RangerServiceNiFiRegistry extends RangerBaseService {

    private static final Log LOG = LogFactory.getLog(RangerServiceNiFiRegistry.class);
    public static final String ACCESS_TYPE_READ  = "read";
    public static final String ACCESS_TYPE_WRITE  = "write";
    public static final String ACCESS_TYPE_DELETE = "delete";

	@Override
	public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceNiFiRegistry.getDefaultRangerPolicies()");
		}

		List<RangerPolicy> ret = super.getDefaultRangerPolicies();
		for (RangerPolicy defaultPolicy : ret) {
			if (defaultPolicy.getName().contains("all") && StringUtils.isNotBlank(lookUpUser)) {
				RangerPolicyItem policyItemForLookupUser = new RangerPolicyItem();
				List<RangerPolicy.RangerPolicyItemAccess> accessListForLookupUser = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();
				accessListForLookupUser.add(new RangerPolicyItemAccess(ACCESS_TYPE_READ));
				accessListForLookupUser.add(new RangerPolicyItemAccess(ACCESS_TYPE_WRITE));
				accessListForLookupUser.add(new RangerPolicyItemAccess(ACCESS_TYPE_DELETE));
				policyItemForLookupUser.setUsers(Collections.singletonList(lookUpUser));
				policyItemForLookupUser.setAccesses(accessListForLookupUser);
				policyItemForLookupUser.setDelegateAdmin(false);
				defaultPolicy.getPolicyItems().add(policyItemForLookupUser);
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceNiFiRegistry.getDefaultRangerPolicies()");
		}
		return ret;
	}

    @Override
    public HashMap<String, Object> validateConfig() throws Exception {
        HashMap<String, Object> ret;
        String serviceName = getServiceName();

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceNiFiRegistry.validateConfig Service: (" + serviceName + " )");
        }

        if (configs != null) {
            try {
                ret = NiFiRegistryConnectionMgr.connectionTest(serviceName, configs);
            } catch (Exception e) {
                LOG.error("<== RangerServiceNiFiRegistry.validateConfig Error:", e);
                throw e;
            }
        } else {
            throw new IllegalStateException("No Configuration found");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceNiFiRegistry.validateConfig Response : (" + ret + " )");
        }

        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        final NiFiRegistryClient client = NiFiRegistryConnectionMgr.getNiFiRegistryClient(serviceName, configs);
        return client.getResources(context);
    }

}
