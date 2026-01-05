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

package org.apache.ranger.biz;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.gds.GdsPolicyEngine;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class GdsPolicyAdminCache {
    private static final Logger LOG = LoggerFactory.getLogger(GdsPolicyAdminCache.class);

    @Autowired
    ServiceDBStore svcStore;

    private volatile GdsPolicies policies;

    public GdsPolicyAdminCache() {
    }

    public boolean isDatasetSharedWith(Long datasetId, String user, Collection<String> groups, Collection<String> roles) {
        boolean            ret      = false;
        List<RangerPolicy> policies = getDatasetPolicies(datasetId);

        for (RangerPolicy policy : policies) {
            ret = hasReference(policy, user, groups, roles);

            if (ret) {
                break;
            }
        }

        LOG.debug("isDatasetSharedWith(datasetId={}, user={}, groups={}, roles={}): policies={}, ret={}", datasetId, user, groups, roles, policies, ret);

        return ret;
    }

    public boolean isProjectSharedWith(Long projectId, String user, Collection<String> groups, Collection<String> roles) {
        boolean            ret      = false;
        List<RangerPolicy> policies = getProjectPolicies(projectId);

        for (RangerPolicy policy : policies) {
            ret = hasReference(policy, user, groups, roles);

            if (ret) {
                break;
            }
        }

        LOG.debug("isProjectSharedWith(datasetId={}, user={}, groups={}, roles={}): policies={}, ret={}", projectId, user, groups, roles, policies, ret);

        return ret;
    }

    private boolean hasReference(RangerPolicy policy, String user, Collection<String> groups, Collection<String> roles) {
        boolean ret = false;

        if (policy.getPolicyItems() != null) {
            for (RangerPolicyItem policyItem : policy.getPolicyItems()) {
                if (policyItem == null) {
                    continue;
                }

                ret = policyItem.getUsers() != null && policyItem.getUsers().contains(user);

                if (!ret && policyItem.getGroups() != null) {
                    ret = policyItem.getGroups().contains(RangerPolicyEngine.GROUP_PUBLIC);

                    if (!ret && groups != null) {
                        ret = CollectionUtils.containsAny(groups, policyItem.getGroups());
                    }
                }

                if (!ret && roles != null && policyItem.getRoles() != null) {
                    ret = CollectionUtils.containsAny(roles, policyItem.getRoles());
                }

                if (ret) {
                    break;
                }
            }
        }

        LOG.debug("hasReference(policy={}, user={}, groups={}, roles={}):, ret={}", policy, user, groups, roles, ret);

        return ret;
    }

    private List<RangerPolicy> getDatasetPolicies(Long datasetId) {
        GdsPolicies        policies = getLatestGdsPolicies();
        List<RangerPolicy> ret      = policies != null && policies.datasetPolicies != null ? policies.datasetPolicies.get(datasetId) : Collections.emptyList();

        return ret != null ? ret : Collections.emptyList();
    }

    private List<RangerPolicy> getProjectPolicies(Long projectId) {
        GdsPolicies        policies = getLatestGdsPolicies();
        List<RangerPolicy> ret      = policies != null && policies.projectPolicies != null ? policies.projectPolicies.get(projectId) : Collections.emptyList();

        return ret != null ? ret : Collections.emptyList();
    }

    private synchronized GdsPolicies getLatestGdsPolicies() {
        try {
            Long            currentVersion = policies != null ? policies.policiesVersion : null;
            ServicePolicies latestPolicies = svcStore.getServicePoliciesIfUpdated(GdsPolicyEngine.GDS_SERVICE_NAME, currentVersion != null ? currentVersion : -1L, true);

            if (latestPolicies != null && latestPolicies.getPolicies() != null) {
                Long                          latestVersion   = latestPolicies.getPolicyVersion();
                Map<Long, List<RangerPolicy>> datasetPolicies = new HashMap<>();
                Map<Long, List<RangerPolicy>> projectPolicies = new HashMap<>();

                LOG.info("refreshing GDS policies from version {} to version {}", currentVersion, latestVersion);

                for (RangerPolicy policy : latestPolicies.getPolicies()) {
                    if (policy != null && (policy.getIsEnabled() == null || Boolean.TRUE.equals(policy.getIsEnabled())) && policy.getResources() != null) {
                        if (policy.getResources().containsKey(GdsPolicyEngine.RESOURCE_NAME_DATASET_ID)) {
                            Long datasetId = getId(policy.getResources().get(GdsPolicyEngine.RESOURCE_NAME_DATASET_ID));

                            if (datasetId == null) {
                                LOG.warn("getLatestGdsPolicies(): ignoring dataset policy id={} - failed to get dataset-id from resource={}", policy.getId(), policy.getResources());
                                continue;
                            }

                            datasetPolicies.computeIfAbsent(datasetId, k -> new ArrayList<>()).add(policy);
                        } else if (policy.getResources().containsKey(GdsPolicyEngine.RESOURCE_NAME_PROJECT_ID)) {
                            Long projectId = getId(policy.getResources().get(GdsPolicyEngine.RESOURCE_NAME_PROJECT_ID));

                            if (projectId == null) {
                                LOG.warn("getLatestGdsPolicies(): ignoring project policy id={} - failed to get project-id from resource={}", policy.getId(), policy.getResources());
                                continue;
                            }

                            projectPolicies.computeIfAbsent(projectId, k -> new ArrayList<>()).add(policy);
                        } else {
                            LOG.warn("getLatestGdsPolicies(): ignoring policy id={} - does not have dataset-id or project-id in resource={}", policy.getId(), policy.getResources());
                        }
                    }
                }

                this.policies = new GdsPolicies(latestVersion, datasetPolicies, projectPolicies);
            } else {
                LOG.debug("getLatestGdsPolicies(): no changes since currentVersion={}", currentVersion);
            }
        } catch (Exception excp) {
            LOG.error("getLatestGdsPolicies(): failed to get policies", excp);
        }

        return this.policies;
    }

    private Long getId(RangerPolicy.RangerPolicyResource policyResource) {
        Long ret = null;

        if (policyResource != null && policyResource.getValues() != null) {
            for (String value : policyResource.getValues()) {
                try {
                    ret = Long.parseLong(value);
                } catch (NumberFormatException excp) {
                    // ignore
                }

                if (ret != null) {
                    break;
                }
            }

            if (ret == null) {
                LOG.warn("getId(): failed to parse dataset/project id from resource-value={}", policyResource.getValues());
            }
        }

        return ret;
    }

    private static class GdsPolicies {
        private final Long                          policiesVersion;
        private final Map<Long, List<RangerPolicy>> datasetPolicies;
        private final Map<Long, List<RangerPolicy>> projectPolicies;

        public GdsPolicies(Long policiesVersion, Map<Long, List<RangerPolicy>> datasetPolicies, Map<Long, List<RangerPolicy>> projectPolicies) {
            this.policiesVersion = policiesVersion;
            this.datasetPolicies = datasetPolicies;
            this.projectPolicies = projectPolicies;
        }
    }
}
