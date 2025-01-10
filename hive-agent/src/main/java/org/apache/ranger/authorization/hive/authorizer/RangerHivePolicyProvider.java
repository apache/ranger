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

package org.apache.ranger.authorization.hive.authorizer;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePolicyChangeListener;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePolicyProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveResourceACLs;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveResourceACLs.AccessResult;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveResourceACLs.Privilege;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.service.RangerAuthContextListener;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RangerHivePolicyProvider implements HivePolicyProvider {
    private static final Logger LOG                              = LoggerFactory.getLogger(RangerHivePolicyProvider.class);
    private static final Logger PERF_HIVEACLPROVIDER_REQUEST_LOG = RangerPerfTracer.getPerfLogger("hiveACLProvider.request");

    private final RangerHiveAuthContextListener authContextListener = new RangerHiveAuthContextListener();
    private final Set<String>                   hivePrivileges;
    private final RangerBasePlugin              rangerPlugin;
    private final RangerHiveAuthorizer          authorizer;

    public RangerHivePolicyProvider(@NotNull RangerBasePlugin hivePlugin, @NotNull RangerHiveAuthorizer authorizer) {
        Set<String> privileges = new HashSet<>();

        for (Privilege privilege : Privilege.values()) {
            privileges.add(privilege.name().toLowerCase());
        }

        this.hivePrivileges = new HashSet<>(privileges);
        this.rangerPlugin   = hivePlugin;
        this.authorizer     = authorizer;
    }

    @Override
    public HiveResourceACLs getResourceACLs(HivePrivilegeObject hiveObject) {
        RangerPerfTracer perf = null;

        if (RangerPerfTracer.isPerfTraceEnabled(PERF_HIVEACLPROVIDER_REQUEST_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_HIVEACLPROVIDER_REQUEST_LOG, "RangerHivePolicyProvider.getResourceACLS()");
        }

        // Extract and build RangerHiveResource from inputObject
        RangerHiveResource hiveResource = authorizer.createHiveResource(hiveObject);
        HiveResourceACLs   ret          = getResourceACLs(hiveResource);

        RangerPerfTracer.log(perf);

        return ret;
    }

    @Override
    public void registerHivePolicyChangeListener(HivePolicyChangeListener listener) {
        LOG.debug("==> RangerHiveACLProviderFactory.registerACLProviderChangeListener()");

        authContextListener.providerChangeListeners.add(listener);

        rangerPlugin.registerAuthContextEventListener(authContextListener);

        LOG.debug("<== RangerHiveACLProviderFactory.registerACLProviderChangeListener()");
    }

    public HiveResourceACLs getResourceACLs(RangerHiveResource hiveResource) {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl(hiveResource, RangerPolicyEngine.ANY_ACCESS, null, null, null);
        RangerResourceACLs      acls    = rangerPlugin.getResourceACLs(request);

        if (LOG.isDebugEnabled()) {
            LOG.debug("HiveResource:[{}], Computed ACLS:[{}]", hiveResource.getAsString(), acls);
        }

        Map<String, Map<Privilege, AccessResult>> userPermissions  = convertRangerACLsToHiveACLs(acls.getUserACLs());
        Map<String, Map<Privilege, AccessResult>> groupPermissions = convertRangerACLsToHiveACLs(acls.getGroupACLs());

        return new RangerHiveResourceACLs(userPermissions, groupPermissions);
    }

    private Map<String, Map<Privilege, AccessResult>> convertRangerACLsToHiveACLs(Map<String, Map<String, RangerResourceACLs.AccessResult>> rangerACLs) {
        Map<String, Map<Privilege, AccessResult>> ret = new HashMap<>();

        if (MapUtils.isNotEmpty(rangerACLs)) {
            for (Map.Entry<String, Map<String, RangerResourceACLs.AccessResult>> entry : rangerACLs.entrySet()) {
                Map<Privilege, AccessResult> permissions = new HashMap<>();

                ret.put(entry.getKey(), permissions);

                for (Map.Entry<String, RangerResourceACLs.AccessResult> permission : entry.getValue().entrySet()) {
                    if (hivePrivileges.contains(permission.getKey())) {
                        Privilege    privilege         = Privilege.valueOf(StringUtils.upperCase(permission.getKey()));
                        int          rangerResultValue = permission.getValue().getResult();
                        AccessResult accessResult;

                        if (rangerResultValue == RangerPolicyEvaluator.ACCESS_ALLOWED) {
                            accessResult = AccessResult.ALLOWED;
                        } else if (rangerResultValue == RangerPolicyEvaluator.ACCESS_DENIED) {
                            accessResult = AccessResult.NOT_ALLOWED;
                        } else if (rangerResultValue == RangerPolicyEvaluator.ACCESS_CONDITIONAL) {
                            accessResult = AccessResult.CONDITIONAL_ALLOWED;
                        } else {
                            // Should not get here
                            accessResult = AccessResult.NOT_ALLOWED;
                        }

                        permissions.put(privilege, accessResult);
                    }
                }
            }
        }

        return ret;
    }

    static class RangerHiveAuthContextListener implements RangerAuthContextListener {
        Set<HivePolicyChangeListener> providerChangeListeners = new HashSet<>();

        public void contextChanged() {
            for (HivePolicyChangeListener eventListener : providerChangeListeners) {
                eventListener.notifyPolicyChange(null);
            }
        }
    }
}
