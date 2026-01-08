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
package org.apache.ranger.services.seatunnel;

import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.seatunnel.client.SeatunnelResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerServiceSeatunnel extends RangerBaseService {
  private static final Logger LOG = LoggerFactory.getLogger(RangerServiceSeatunnel.class);

  @Override
  public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("==> RangerServiceSeatunnel.getDefaultRangerPolicies()");
    }

    List<RangerPolicy> defaultPolicies = new ArrayList<>();
    String adminUser = "admin"; // Replace with the actual admin username if different

    // Create a combined policy for the 'user' and 'workspace' resource types
    RangerPolicy combinedPolicy = new RangerPolicy();
    combinedPolicy.setName("admin-all-user-workspace");
    combinedPolicy.setService(getServiceName());

    Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
    resources.put("user", new RangerPolicy.RangerPolicyResource("*"));



    combinedPolicy.setResources(resources);

    Map<String, RangerPolicy.RangerPolicyResource> additionalResources = new HashMap<>();
    additionalResources.put("workspace", new RangerPolicy.RangerPolicyResource("*"));
    combinedPolicy.addResource(additionalResources);

    combinedPolicy.setPolicyItems(Collections.singletonList(createPolicyItem(adminUser)));
    defaultPolicies.add(combinedPolicy);

    if (LOG.isDebugEnabled()) {
      LOG.debug("<== RangerServiceSeatunnel.getDefaultRangerPolicies()");
    }
    return defaultPolicies;
  }

  private RangerPolicyItem createPolicyItem(String username) {
    RangerPolicyItem policyItem = new RangerPolicyItem();
    policyItem.setUsers(Collections.singletonList(username));
    policyItem.setAccesses(createAllAccesses());
    policyItem.setDelegateAdmin(true);
    return policyItem;
  }

  private List<RangerPolicyItemAccess> createAllAccesses() {
    List<RangerPolicyItemAccess> accesses = new ArrayList<>();
    accesses.add(new RangerPolicyItemAccess("create"));
    accesses.add(new RangerPolicyItemAccess("read"));
    accesses.add(new RangerPolicyItemAccess("update"));
    accesses.add(new RangerPolicyItemAccess("delete"));
    accesses.add(new RangerPolicyItemAccess("execute"));
    return accesses;
  }

  @Override
  public Map<String, Object> validateConfig() throws Exception {
    Map<String, Object> ret = new HashMap<>();
    String serviceName = getServiceName();

    if (LOG.isDebugEnabled()) {
        LOG.debug("RangerServiceSeatunnel.validateConfig(): Service: {}", serviceName);
    }

    if (configs != null) {
      try {
        ret = SeatunnelResourceManager.connectionTest(serviceName, configs);
      } catch (HadoopException he) {
        LOG.error("<== RangerServiceSeatunnel.validateConfig() Error:", he);
        throw he;
      }
    }

    if (LOG.isDebugEnabled()) {
        LOG.debug("RangerServiceSeatunnel.validateConfig(): Response: {}", ret);
    }
    return ret;
  }

  @Override
  public List<String> lookupResource(ResourceLookupContext context) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("==> RangerServiceSeatunnel.lookupResource() Context: ({})", context);
    }

    List<String> ret = new ArrayList<>();
    if (context != null) {
      try {
        ret = SeatunnelResourceManager.getSeatunnelResources(getServiceName(), getServiceType(), getConfigs(), context);
      } catch (Exception e) {
        LOG.error("<==RangerServiceSeatunnel.lookupResource() Error : ", e);
        throw e;
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("<== RangerServiceSeatunnel.lookupResource() Response: ({})", ret);
    }
    return ret;
  }

}
