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

package org.apache.ranger.sizing;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class PerfRequestGenerator {
    private final Set<String> resourceKeys;

    public PerfRequestGenerator(Set<String> resourceKeys) {
        this.resourceKeys = resourceKeys != null ? resourceKeys : Collections.emptySet();
    }

    public Collection<RangerAccessRequest> generate(ServicePolicies policies, ServiceTags tags) {
        List<RangerAccessRequest> ret         = new ArrayList<>();
        Set<Map<String, Object>>  resources   = new HashSet<>();
        Set<String>               accessTypes = new HashSet<>();
        Set<String>               users       = new HashSet<>();
        Set<String>               groups      = new HashSet<>();

        // collect resources, users, groups referenced in policies
        // collect accessTypes from serviceDef
        if (policies != null) {
            if (policies.getServiceDef() != null && policies.getServiceDef().getAccessTypes() != null) {
                policies.getServiceDef().getAccessTypes().forEach(atd -> accessTypes.add(atd.getName()));
            }

            if (policies.getPolicies() != null) {
                policies.getPolicies().forEach(policy -> {
                    collectResources(policy.getResources(), resources);

                    if (policy.getAdditionalResources() != null) {
                        policy.getAdditionalResources().forEach(resource -> collectResources(resource, resources));
                    }

                    collectUsersGroups(policy, users, groups);
                });
            }

            if (policies.getTagPolicies() != null && policies.getTagPolicies().getPolicies() != null) {
                policies.getTagPolicies().getPolicies().forEach(policy -> collectUsersGroups(policy, users, groups));
            }
        }

        // collect resources from tagged-resources
        if (tags != null && tags.getServiceResources() != null) {
            tags.getServiceResources().forEach(resource -> collectResources(resource.getResourceElements(), resources));
        }

        if (accessTypes.isEmpty()) {
            accessTypes.add("read");
        }

        if (users.isEmpty()) {
            users.add("user1");
        }

        if (groups.isEmpty()) {
            groups.add("group1");
        }

        Iterator<String> iterAccessTypes = accessTypes.iterator();
        Iterator<String> iterUser        = users.iterator();
        Iterator<String> iterGroup       = groups.iterator();

        for (Map<String, Object> resource : resources) {
            String      accessType = iterAccessTypes.next();
            String      user       = iterUser.next();
            Set<String> userGroups = Collections.singleton(iterGroup.next());

            ret.add(new RangerAccessRequestImpl(new RangerAccessResourceImpl(resource), accessType, user, userGroups, null));

            if (!iterAccessTypes.hasNext()) {
                iterAccessTypes = accessTypes.iterator();
            }

            if (!iterUser.hasNext()) {
                iterUser = users.iterator();
            }

            if (!iterGroup.hasNext()) {
                iterGroup = groups.iterator();
            }
        }

        return ret;
    }

    private void collectResources(Map<String, RangerPolicyResource> policyResource, Set<Map<String, Object>> resources) {
        if (policyResource != null) {
            int resourceCount = 1;

            for (Map.Entry<String, RangerPolicyResource> entry : policyResource.entrySet()) {
                String       name   = entry.getKey();
                List<String> values = entry.getValue().getValues();

                if (!values.isEmpty() && (resourceKeys.isEmpty() || resourceKeys.contains(name))) {
                    resourceCount *= values.size();
                }
            }

            List<Map<String, Object>> toAdd = new ArrayList<>(resourceCount);

            for (int i = 0; i < resourceCount; i++) {
                toAdd.add(new HashMap<>());
            }

            for (Map.Entry<String, RangerPolicyResource> entry : policyResource.entrySet()) {
                String       name   = entry.getKey();
                List<String> values = entry.getValue().getValues();

                if (!values.isEmpty() && (resourceKeys.isEmpty() || resourceKeys.contains(name))) {
                    for (int idxResource = 0; idxResource < resourceCount; idxResource++) {
                        toAdd.get(idxResource).put(name, values.get(idxResource % values.size()));
                    }
                }
            }

            resources.addAll(toAdd);
        }
    }

    private void collectUsersGroups(RangerPolicy policy, Set<String> users, Set<String> groups) {
        collectUsersGroups(policy.getPolicyItems(), users, groups);
        collectUsersGroups(policy.getAllowExceptions(), users, groups);
        collectUsersGroups(policy.getDenyPolicyItems(), users, groups);
        collectUsersGroups(policy.getDenyExceptions(), users, groups);
        collectUsersGroups(policy.getDataMaskPolicyItems(), users, groups);
        collectUsersGroups(policy.getRowFilterPolicyItems(), users, groups);
    }

    private void collectUsersGroups(List<? extends RangerPolicyItem> policyItems, Set<String> users, Set<String> groups) {
        if (policyItems != null) {
            policyItems.forEach(policyItem -> {
                users.addAll(policyItem.getUsers());
                groups.addAll(policyItem.getGroups());
            });
        }
    }
}
