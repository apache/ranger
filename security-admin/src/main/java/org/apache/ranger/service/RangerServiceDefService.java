/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Scope("singleton")
public class RangerServiceDefService extends RangerServiceDefServiceBase<XXServiceDef, RangerServiceDef> {
    public static final String PROP_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION_FOR_OZONE = "ranger.servicedef.ozone.enableActionMatcherInPoliciesCondition";
    public static final String OPTION_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION = "enableActionMatcherInPoliciesCondition";

    private static final String POLICY_CONDITION_ACTION_MATCHES = "action-matches";

    private final RangerAdminConfig config;

    public RangerServiceDefService() {
        super();

        this.config = RangerAdminConfig.getInstance();
    }

    public List<RangerServiceDef> getAllServiceDefs() {
        List<XXServiceDef>     xxServiceDefList = getDao().getAll();
        List<RangerServiceDef> serviceDefList   = new ArrayList<>();

        for (XXServiceDef xxServiceDef : xxServiceDefList) {
            RangerServiceDef serviceDef = populateViewBean(xxServiceDef);

            serviceDefList.add(serviceDef);
        }

        return serviceDefList;
    }

    public RangerServiceDef getPopulatedViewObject(XXServiceDef xServiceDef) {
        return this.populateViewBean(xServiceDef);
    }

    @Override
    protected void validateForCreate(RangerServiceDef vObj) {
    }

    @Override
    protected void validateForUpdate(RangerServiceDef vObj, XXServiceDef entityObj) {
    }

    @Override
    protected XXServiceDef mapViewToEntityBean(RangerServiceDef vObj, XXServiceDef xObj, int operationContext) {
        return super.mapViewToEntityBean(vObj, xObj, operationContext);
    }

    @Override
    protected RangerServiceDef mapEntityToViewBean(RangerServiceDef vObj, XXServiceDef xObj) {
        RangerServiceDef    ret               = super.mapEntityToViewBean(vObj, xObj);
        Map<String, String> serviceDefOptions = ret.getOptions();

        if (serviceDefOptions.get(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES) == null) {
            boolean enableDenyAndExceptionsInPoliciesHiddenOption = config.getBoolean("ranger.servicedef.enableDenyAndExceptionsInPolicies", true);

            if (enableDenyAndExceptionsInPoliciesHiddenOption || StringUtils.equalsIgnoreCase(ret.getName(), EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME)) {
                serviceDefOptions.put(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES, "true");
            } else {
                serviceDefOptions.put(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES, "false");
            }

            ret.setOptions(serviceDefOptions);
        }

        if (serviceDefOptions.get(RangerServiceDef.OPTION_ENABLE_TAG_BASED_POLICIES) == null) {
            boolean enableTagBasedPoliciesHiddenOption = config.getBoolean("ranger.servicedef.enableTagBasedPolicies", true);

            if (enableTagBasedPoliciesHiddenOption) {
                serviceDefOptions.put(RangerServiceDef.OPTION_ENABLE_TAG_BASED_POLICIES, "true");
            } else {
                serviceDefOptions.put(RangerServiceDef.OPTION_ENABLE_TAG_BASED_POLICIES, "false");
            }

            ret.setOptions(serviceDefOptions);
        }

        return ret;
    }

    @Override
    protected RangerServiceDef populateViewBean(XXServiceDef xServiceDef) {
        final RangerServiceDef ret = super.populateViewBean(xServiceDef);

        applyActionMatcherInPoliciesConditionHiddenOption(ret);

        return ret;
    }

    void applyActionMatcherInPoliciesConditionHiddenOption(RangerServiceDef serviceDef) {
        if (serviceDef == null) {
            return;
        }

        final boolean isOzoneServiceDef = StringUtils.equalsIgnoreCase(serviceDef.getName(), EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME);

        if (!isOzoneServiceDef) {
            return;
        }

        Map<String, String> serviceDefOptions = serviceDef.getOptions();

        if (serviceDefOptions == null) {
            serviceDefOptions = new HashMap<>();
            serviceDef.setOptions(serviceDefOptions);
        }

        // Admin site config is authoritative at runtime; stale def_options must not override it.
        boolean enabled = config.getBoolean(PROP_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION_FOR_OZONE, false);

        serviceDefOptions.put(OPTION_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION, Boolean.toString(enabled));
        serviceDef.setOptions(serviceDefOptions);

        if (enabled) {
            ensureActionMatchesPolicyCondition(serviceDef);
        } else {
            removeActionMatchesPolicyCondition(serviceDef);
        }
    }

    private void ensureActionMatchesPolicyCondition(RangerServiceDef serviceDef) {
        if (hasActionMatchesPolicyCondition(serviceDef)) {
            return;
        }

        try {
            final RangerServiceDef embeddedOzoneServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_OZONE_NAME);

            if (embeddedOzoneServiceDef == null || embeddedOzoneServiceDef.getPolicyConditions() == null) {
                return;
            }

            for (RangerPolicyConditionDef embeddedPolicyConditionDef : embeddedOzoneServiceDef.getPolicyConditions()) {
                if (StringUtils.equals(embeddedPolicyConditionDef.getName(), POLICY_CONDITION_ACTION_MATCHES)) {
                    List<RangerPolicyConditionDef> policyConditions = serviceDef.getPolicyConditions();

                    if (policyConditions == null) {
                        policyConditions = new ArrayList<>();
                        serviceDef.setPolicyConditions(policyConditions);
                    }

                    policyConditions.add(embeddedPolicyConditionDef);

                    return;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to restore ozone action-matches policy condition", e);
        }
    }

    private void removeActionMatchesPolicyCondition(RangerServiceDef serviceDef) {
        List<RangerPolicyConditionDef> policyConditions = serviceDef.getPolicyConditions();

        if (policyConditions == null || policyConditions.isEmpty()) {
            return;
        }

        List<RangerPolicyConditionDef> filteredPolicyConditions = new ArrayList<>();

        for (RangerPolicyConditionDef policyConditionDef : policyConditions) {
            if (!StringUtils.equals(policyConditionDef.getName(), POLICY_CONDITION_ACTION_MATCHES)) {
                filteredPolicyConditions.add(policyConditionDef);
            }
        }

        serviceDef.setPolicyConditions(filteredPolicyConditions);
    }

    private static boolean hasActionMatchesPolicyCondition(RangerServiceDef serviceDef) {
        List<RangerPolicyConditionDef> policyConditions = serviceDef.getPolicyConditions();

        if (policyConditions == null || policyConditions.isEmpty()) {
            return false;
        }

        for (RangerPolicyConditionDef policyConditionDef : policyConditions) {
            if (StringUtils.equals(policyConditionDef.getName(), POLICY_CONDITION_ACTION_MATCHES)) {
                return true;
            }
        }

        return false;
    }
}
