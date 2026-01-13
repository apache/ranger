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

package org.apache.ranger.authz.embedded;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authz.api.RangerAuthzApiErrorCode;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.model.RangerAccessContext;
import org.apache.ranger.authz.model.RangerAccessInfo;
import org.apache.ranger.authz.model.RangerAuthzRequest;
import org.apache.ranger.authz.model.RangerAuthzResult;
import org.apache.ranger.authz.model.RangerAuthzResult.AccessDecision;
import org.apache.ranger.authz.model.RangerAuthzResult.AccessResult;
import org.apache.ranger.authz.model.RangerAuthzResult.DataMaskResult;
import org.apache.ranger.authz.model.RangerAuthzResult.PermissionResult;
import org.apache.ranger.authz.model.RangerAuthzResult.PolicyInfo;
import org.apache.ranger.authz.model.RangerAuthzResult.ResultInfo;
import org.apache.ranger.authz.model.RangerAuthzResult.RowFilterResult;
import org.apache.ranger.authz.model.RangerResourceInfo;
import org.apache.ranger.authz.model.RangerResourcePermissions;
import org.apache.ranger.authz.model.RangerUserInfo;
import org.apache.ranger.authz.util.RangerResourceNameParser;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator.ACCESS_ALLOWED;
import static org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator.ACCESS_CONDITIONAL;
import static org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator.ACCESS_DENIED;

public class RangerAuthzPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAuthzPlugin.class);

    private final RangerBasePlugin                      plugin;
    private final Map<String, RangerResourceNameParser> rrnTemplates = new HashMap<>();

    public RangerAuthzPlugin(String serviceType, String serviceName, Properties properties) {
        plugin = new RangerBasePlugin(getPluginConfig(serviceType, serviceName, properties)) {
            @Override
            public void setPolicies(ServicePolicies policies) {
                super.setPolicies(policies);

                updateResourceTemplates();
            }
        };

        plugin.init();
    }

    public void cleanup() {
        plugin.cleanup();
    }

    public RangerAuthzResult authorize(RangerAuthzRequest request, RangerAuthzAuditHandler auditHandler) throws RangerAuthzException {
        RangerUserInfo          userInfo      = request.getUser();
        RangerAccessInfo        access        = request.getAccess();
        RangerAccessContext     context       = request.getContext();
        Set<String>             permissions   = access.getPermissions();
        RangerAuthzResult       ret           = new RangerAuthzResult(request.getRequestId(), new HashMap<>(permissions.size()));
        RangerAccessResource    resource      = getResource(access.getResource().getName(), access.getResource().getAttributes());
        RangerAccessRequestImpl accessRequest = new RangerAccessRequestImpl(resource, null, userInfo.getName(), userInfo.getGroups(), userInfo.getRoles());

        initializeRequest(accessRequest, context);

        boolean hasDeny          = false;
        boolean hasAllow         = false;
        boolean hasNotDetermined = false;

        for (String permission : permissions) {
            accessRequest.setAccessType(permission);
            accessRequest.setContext(new HashMap<>(context.getAdditionalInfo()));

            ResultInfo       result     = evaluate(accessRequest, auditHandler);
            PermissionResult permResult = new PermissionResult(permission, result);

            if (CollectionUtils.isNotEmpty(access.getResource().getSubResources())) {
                permResult.setAccess(new AccessResult()); // when sub-resources are evaluated, top-level resource's access is derived from sub-resources
                permResult.setSubResources(new HashMap<>(access.getResource().getSubResources().size()));

                for (String subResourceName : access.getResource().getSubResources()) {
                    accessRequest.setResource(getSubResource(resource, subResourceName));
                    accessRequest.setContext(new HashMap<>(context.getAdditionalInfo())); // reset the context

                    ResultInfo subResourceResult = evaluate(accessRequest, auditHandler);

                    updateResult(subResourceResult.getAccess(), permResult.getAccess());

                    permResult.getSubResources().put(subResourceName, subResourceResult);
                }
            }

            if (permResult.getAccess().getDecision() == AccessDecision.DENY) {
                permResult.setDataMask(null);
                permResult.setRowFilter(null);
            }

            ret.getPermissions().put(permission, permResult);

            AccessDecision permDecision = permResult.getAccess() == null ? AccessDecision.NOT_DETERMINED : permResult.getAccess().getDecision();

            if (permDecision == AccessDecision.DENY) {
                hasDeny = true;
            } else if (permDecision == AccessDecision.ALLOW) {
                hasAllow = true;
            } else {
                hasNotDetermined = true;
            }
        }

        if (hasDeny) {
            ret.setDecision(AccessDecision.DENY);
        } else if (hasNotDetermined) {
            ret.setDecision(AccessDecision.NOT_DETERMINED);
        } else if (hasAllow) {
            ret.setDecision(AccessDecision.ALLOW);
        }

        return ret;
    }

    public RangerResourcePermissions getResourcePermissions(RangerResourceInfo resource, RangerAccessContext context) throws RangerAuthzException {
        RangerResourcePermissions ret     = new RangerResourcePermissions();
        RangerAccessRequestImpl   request = new RangerAccessRequestImpl();

        ret.setResource(resource);
        request.setResource(getResource(resource.getName(), null));
        initializeRequest(request, context);

        RangerResourceACLs acls = plugin.getResourceACLs(request);

        if (acls != null) {
            for (Map.Entry<String, Map<String, RangerResourceACLs.AccessResult>> entry : acls.getUserACLs().entrySet()) {
                String                                       userName = entry.getKey();
                Map<String, RangerResourceACLs.AccessResult> userAcls = entry.getValue();

                if (userAcls != null) {
                    for (Map.Entry<String, RangerResourceACLs.AccessResult> aclEntry : userAcls.entrySet()) {
                        String                          permission = aclEntry.getKey();
                        RangerResourceACLs.AccessResult acl        = aclEntry.getValue();

                        ret.setUserPermission(userName, permission, new PermissionResult(permission, new ResultInfo(toAccessResult(acl), null, null, null)));
                    }
                }
            }

            for (Map.Entry<String, Map<String, RangerResourceACLs.AccessResult>> entry : acls.getGroupACLs().entrySet()) {
                String                                       groupName = entry.getKey();
                Map<String, RangerResourceACLs.AccessResult> groupAcls = entry.getValue();

                if (groupAcls != null) {
                    for (Map.Entry<String, RangerResourceACLs.AccessResult> aclEntry : groupAcls.entrySet()) {
                        String                          permission = aclEntry.getKey();
                        RangerResourceACLs.AccessResult acl        = aclEntry.getValue();

                        ret.setGroupPermission(groupName, permission, new PermissionResult(permission, new ResultInfo(toAccessResult(acl), null, null, null)));
                    }
                }
            }

            for (Map.Entry<String, Map<String, RangerResourceACLs.AccessResult>> entry : acls.getRoleACLs().entrySet()) {
                String                                       roleName = entry.getKey();
                Map<String, RangerResourceACLs.AccessResult> roleAcls = entry.getValue();

                if (roleAcls != null) {
                    for (Map.Entry<String, RangerResourceACLs.AccessResult> aclEntry : roleAcls.entrySet()) {
                        String                          permission = aclEntry.getKey();
                        RangerResourceACLs.AccessResult acl        = aclEntry.getValue();

                        ret.setRolePermission(roleName, permission, new PermissionResult(permission, new ResultInfo(toAccessResult(acl), null, null, null)));
                    }
                }
            }
        }

        return ret;
    }

    RangerBasePlugin getPlugin() {
        return plugin;
    }

    private RangerAccessResource getResource(String resource, Map<String, Object> attributes) throws RangerAuthzException {
        Map<String, Object> resourceMap = getResourceAsMap(resource);
        Object              ownerName   = attributes != null ? attributes.get(RangerAccessRequestUtil.KEY_OWNER) : null;

        return new RangerAccessResourceImpl(resourceMap, ownerName != null ? ownerName.toString() : null);
    }

    private RangerAccessResource getSubResource(RangerAccessResource parent, String subResourceName) {
        Map<String, Object> elements = new HashMap<>(parent.getAsMap());

        if (StringUtils.isNotBlank(subResourceName)) {
            String[] parts = subResourceName.split(":", 2);

            elements.put(parts[0], parts.length > 1 ? parts[1] : "");
        }

        return new RangerAccessResourceImpl(elements, parent.getOwnerUser());
    }

    private void initializeRequest(RangerAccessRequestImpl request, RangerAccessContext context) {
        request.setAccessTime(new Date(context.getAccessTime()));
        request.setClientIPAddress(context.getClientIpAddress());
        request.setForwardedAddresses(context.getForwardedIpAddresses());
        request.setClientType(getClientType(context.getAdditionalInfo()));
        request.setClusterType(getClusterType(context.getAdditionalInfo()));
        request.setClusterName(getClusterName(context.getAdditionalInfo()));
        request.setRequestData(getRequestData(context.getAdditionalInfo()));
        request.setContext(new HashMap<>(context.getAdditionalInfo()));
    }

    private String getClientType(Map<String, Object> context) {
        Object ret = context != null ? context.get(RangerAccessContext.CONTEXT_INFO_CLIENT_TYPE) : null;

        return ret != null ? ret.toString() : null;
    }

    private String getClusterType(Map<String, Object> context) {
        Object ret = context != null ? context.get(RangerAccessContext.CONTEXT_INFO_CLUSTER_TYPE) : null;

        return ret != null ? ret.toString() : null;
    }

    private String getClusterName(Map<String, Object> context) {
        Object ret = context != null ? context.get(RangerAccessContext.CONTEXT_INFO_CLUSTER_NAME) : null;

        return ret != null ? ret.toString() : null;
    }

    private String getRequestData(Map<String, Object> context) {
        Object ret = context != null ? context.get(RangerAccessContext.CONTEXT_INFO_REQUEST_DATA) : null;

        return ret != null ? ret.toString() : null;
    }

    private void updateResult(AccessResult from, AccessResult to) {
        if (from == null || to == null || from.getDecision() == null ||
                to.getDecision() == from.getDecision() || // no change in decision
                to.getDecision() == AccessDecision.DENY) { // don't override earlier DENY
            return;
        }

        if (to.getDecision() == null || from.getDecision() == AccessDecision.DENY || from.getDecision() == AccessDecision.NOT_DETERMINED) {
            to.setDecision(from.getDecision());
            to.setPolicy(from.getPolicy());
        }
    }

    private ResultInfo toPermissionResult(RangerAccessResult result) {
        ResultInfo ret = new ResultInfo(toAccessResult(result), null, null, null);

        if (result.getPolicyType() == RangerPolicy.POLICY_TYPE_DATAMASK) {
            ret.setDataMask(new DataMaskResult(result.getMaskType(), result.getMaskedValue(), ret.getAccess().getPolicy()));
        } else if (result.getPolicyType() == RangerPolicy.POLICY_TYPE_ROWFILTER) {
            ret.setRowFilter(new RowFilterResult(result.getFilterExpr(), ret.getAccess().getPolicy()));
        }

        return ret;
    }

    private AccessResult toAccessResult(RangerResourceACLs.AccessResult result) {
        AccessResult ret = new AccessResult();

        if (result != null) {
            if (result.getIsFinal()) {
                if (result.getResult() == ACCESS_ALLOWED) {
                    ret.setDecision(AccessDecision.ALLOW);
                } else if (result.getResult() == ACCESS_DENIED) {
                    ret.setDecision(AccessDecision.DENY);
                } else if (result.getResult() == ACCESS_CONDITIONAL) {
                    ret.setDecision(AccessDecision.DENY); // TODO: introduce a new AccessDecision.CONDITIONAL
                } else {
                    ret.setDecision(AccessDecision.NOT_DETERMINED);
                }
            } else {
                ret.setDecision(AccessDecision.NOT_DETERMINED);
            }

            if (result.getPolicy() != null) {
                ret.setPolicy(new PolicyInfo(result.getPolicy().getId(), result.getPolicy().getVersion()));
            } else {
                ret.setPolicy(null);
            }
        }

        return ret;
    }

    private AccessResult toAccessResult(RangerAccessResult result) {
        AccessResult ret = new AccessResult();

        if (result.getIsAccessDetermined()) {
            ret.setDecision(result.getIsAllowed() ? AccessDecision.ALLOW : AccessDecision.DENY);
        } else {
            ret.setDecision(AccessDecision.NOT_DETERMINED);
        }

        ret.setPolicy(toPolicyInfo(result));

        return ret;
    }

    private PolicyInfo toPolicyInfo(RangerAccessResult result) {
        return new PolicyInfo(result.getPolicyId(), result.getPolicyVersion());
    }

    private ResultInfo evaluate(RangerAccessRequest request, RangerAuthzAuditHandler auditHandler) {
        RangerAccessResult           result = plugin.isAccessAllowed(request, auditHandler);
        ResultInfo ret    = toPermissionResult(result);

        if (plugin.getServiceDefHelper().isRowFilterSupported(request.getResource().getKeys())) {
            RangerAccessResult rowFilterResult = plugin.evalRowFilterPolicies(request, auditHandler);

            if (rowFilterResult != null && rowFilterResult.getIsAccessDetermined() && StringUtils.isNotBlank(rowFilterResult.getFilterExpr())) {
                ret.setRowFilter(new RowFilterResult(rowFilterResult.getFilterExpr(), toPolicyInfo(rowFilterResult)));
            }
        }

        if (plugin.getServiceDefHelper().isDataMaskSupported(request.getResource().getKeys())) {
            RangerAccessResult dataMaskResult = plugin.evalDataMaskPolicies(request, auditHandler);

            if (dataMaskResult != null && dataMaskResult.getIsAccessDetermined() && StringUtils.isNotBlank(dataMaskResult.getMaskType())) {
                ret.setDataMask(new DataMaskResult(dataMaskResult.getMaskType(), dataMaskResult.getMaskedValue(), toPolicyInfo(dataMaskResult)));
            }
        }

        return ret;
    }

    private Map<String, Object> getResourceAsMap(String resource) throws RangerAuthzException {
        String[]                 resourceParts = resource.split(":", 2);
        String                   resourceType  = resourceParts.length > 0 ? resourceParts[0] : null;
        String                   resourceValue = resourceParts.length > 1 ? resourceParts[1] : null;
        RangerResourceNameParser template      = rrnTemplates.get(resourceType);

        if (template == null) {
            throw new RangerAuthzException(RangerAuthzApiErrorCode.INVALID_REQUEST_RESOURCE_TYPE_NOT_FOUND, resourceType);
        }

        Map ret = template.parseToMap(resourceValue);

        if (ret == null) {
            throw new RangerAuthzException(RangerAuthzApiErrorCode.INVALID_REQUEST_RESOURCE_VALUE_FOR_TYPE, resourceValue, resourceType);
        }

        return (Map<String, Object>) ret;
    }

    private void updateResourceTemplates() {
        RangerServiceDefHelper serviceDefHelper = plugin.getServiceDefHelper();

        if (serviceDefHelper != null) {
            for (String resourceType : serviceDefHelper.getAllResourceNames()) {
                String                   rrnTemplate = serviceDefHelper.getRrnTemplate(resourceType);
                RangerResourceNameParser existing    = rrnTemplates.get(resourceType);

                if (existing == null || !Objects.equals(existing.getTemplate(), rrnTemplate)) {
                    LOG.info("updateResourceTemplates(): resourceType={} updated to rrnTemplate={}", resourceType, rrnTemplate);

                    try {
                        rrnTemplates.put(resourceType, new RangerResourceNameParser(rrnTemplate));
                    } catch (RangerAuthzException excp) {
                        LOG.warn("updateResourceTemplates(): failed to create resource template for resourceType={}, rrnTemplate={}", resourceType, rrnTemplate, excp);
                    }
                }
            }
        }
    }

    private static RangerPluginConfig getPluginConfig(String serviceType, String serviceName, Properties properties) {
        return new RangerPluginConfig(serviceType, serviceName, null, properties);
    }
}
