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

package org.apache.ranger.plugin.policyevaluator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.plugin.model.RangerInlinePolicy;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPrincipal;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestWrapper;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerInlinePolicyEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(RangerInlinePolicyEvaluator.class);

    private static final String PRINCIPAL_G_PUBLIC = RangerPrincipal.PREFIX_GROUP + RangerPolicyEngine.GROUP_PUBLIC;

    private final RangerInlinePolicy   policy;
    private final RangerPolicyEngine   policyEngine;
    private final List<GrantEvaluator> grants;

    public RangerInlinePolicyEvaluator(RangerInlinePolicy policy, RangerPolicyEngine policyEngine) {
        this.policy       = policy;
        this.policyEngine = policyEngine;
        this.grants       = toGrantEvaluators(policy);

        LOG.debug("RangerInlinePolicyEvaluator(policy={})", policy);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    public void evaluate(RangerAccessRequest request, RangerAccessResult result) {
        LOG.debug("==> RangerInlinePolicyEvaluator.evaluate({}, {}, {})", policy, request, result);

        if (request != null && result != null) {
            boolean isAllowed = result.getIsAccessDetermined() && result.getIsAllowed();
            boolean evalInlinePolicy;

            if (policy.getMode() == null || policy.getMode() == RangerInlinePolicy.Mode.INLINE) {
                evalInlinePolicy = true; // request must be allowed by the inline policy
            } else if (policy.getMode() == RangerInlinePolicy.Mode.RANGER_AND_INLINE) {
                evalInlinePolicy = isAllowed; // if not allowed by Ranger policies, no need to evaluate inline policy
            } else { // RANGER_OR_ONLINE
                evalInlinePolicy = !isAllowed; // if already allowed by Ranger policies, no need to evaluate inline policy
            }

            if (evalInlinePolicy) {
                isAllowed = isAllowed(request);

                result.setIsAllowed(isAllowed);
                result.setIsAccessDetermined(true);
                result.setPolicyId(-1);
                result.setPolicyVersion(null);
                result.setReason("inline-policy");
            }
        }

        LOG.debug("<== RangerInlinePolicyEvaluator.evaluate({}, {}, {})", policy, request, result);
    }

    private boolean isAllowed(RangerAccessRequest request) {
        boolean ret = isAllowedForGrantor(request);

        if (ret && !grants.isEmpty()) {
            for (GrantEvaluator evaluator : grants) {
                ret = evaluator.isAllowed(request);

                if (ret) {
                    break;
                }
            }
        }

        return ret;
    }

    private boolean isAllowedForGrantor(RangerAccessRequest request) {
        final boolean         ret;
        final RangerPrincipal grantor = RangerPrincipal.toPrincipal(policy.getGrantor());

        if (grantor != null) {
            try (RangerGrantorAccessRequest grantorAccessReq = new RangerGrantorAccessRequest(request, grantor)) {
                RangerAccessResult result = policyEngine.evaluatePolicies(grantorAccessReq, RangerPolicy.POLICY_TYPE_ACCESS, null);

                ret = result != null && result.getIsAccessDetermined() && result.getIsAllowed();
            }
        } else {
            ret = true;
        }

        return ret;
    }

    private StringBuilder toString(StringBuilder sb) {
        sb.append("RangerInlinePolicyEvaluator={");

        sb.append("policy={").append(policy).append("}");

        sb.append("}");

        return sb;
    }

    private List<GrantEvaluator> toGrantEvaluators(RangerInlinePolicy policy) {
        if (CollectionUtils.isEmpty(policy.getGrants())) {
            return Collections.emptyList();
        }

        List<GrantEvaluator> ret = new ArrayList<>(policy.getGrants().size());

        for (RangerInlinePolicy.Grant grant : policy.getGrants()) {
            GrantEvaluator evaluator = new GrantEvaluator(grant);

            ret.add(evaluator);
        }

        return ret;
    }

    private class GrantEvaluator {
        private final RangerInlinePolicy.Grant         grant;
        private final Set<String>                      permissions;
        private final Set<RangerPolicyResourceMatcher> resourceMatchers = new HashSet<>();

        public GrantEvaluator(RangerInlinePolicy.Grant grant) {
            this.grant       = grant;
            this.permissions = policyEngine.getServiceDefHelper().expandImpliedAccessGrants(grant.getPermissions());

            if (grant.getResources() != null) {
                for (String resource : grant.getResources()) {
                    try {
                        Map<String, RangerPolicyResource> policyResources = policyEngine.getServiceDefHelper().parseResourceToPolicyResources(resource);

                        RangerDefaultPolicyResourceMatcher resourceMatcher = new RangerDefaultPolicyResourceMatcher();

                        resourceMatcher.setServiceDef(policyEngine.getServiceDef());
                        resourceMatcher.setPolicyResources(policyResources, RangerPolicy.POLICY_TYPE_ACCESS);
                        resourceMatcher.setPluginContext(policyEngine.getPluginContext());

                        resourceMatcher.setServiceDefHelper(policyEngine.getServiceDefHelper());
                        resourceMatcher.init();

                        resourceMatchers.add(resourceMatcher);
                    } catch (RangerAuthzException excp) {
                        LOG.debug("GrantEvaluator(): invalid resource {}", resource);
                    }
                }
            }

            LOG.debug("RangerGrantEvaluator(grant={})", grant);
        }

        public boolean isAllowed(RangerAccessRequest request) {
            boolean ret = isPrincipalMatch(request) && isPermissionMatch(request) && isResourceMatch(request);

            LOG.debug("isAllowed(grant={}, request={}): ret={}", grant, request, ret);

            return ret;
        }

        private boolean isPrincipalMatch(RangerAccessRequest request) {
            boolean ret = CollectionUtils.isEmpty(grant.getPrincipals()) || grant.getPrincipals().contains(PRINCIPAL_G_PUBLIC); // match all users;

            if (!ret) {
                if (StringUtils.isNotBlank(request.getUser())) {
                    ret = grant.getPrincipals().contains(RangerPrincipal.PREFIX_USER + request.getUser());
                }

                if (!ret && CollectionUtils.isNotEmpty(request.getUserGroups())) {
                    for (String groupName : request.getUserGroups()) {
                        ret = grant.getPrincipals().contains(RangerPrincipal.PREFIX_GROUP + groupName);

                        if (ret) {
                            break;
                        }
                    }
                }

                if (!ret && CollectionUtils.isNotEmpty(request.getUserRoles())) {
                    for (String roleName : request.getUserRoles()) {
                        ret = grant.getPrincipals().contains(RangerPrincipal.PREFIX_ROLE + roleName);

                        if (ret) {
                            break;
                        }
                    }
                }
            }

            LOG.debug("isPrincipalMatch(grant={}, request={}): ret={}", grant, request, ret);

            return ret;
        }

        private boolean isPermissionMatch(RangerAccessRequest request) {
            boolean ret = StringUtils.isNotBlank(request.getAccessType()) &&
                    CollectionUtils.isNotEmpty(permissions) &&
                    permissions.contains(request.getAccessType());

            LOG.debug("isPermissionMatch(grant={}, request={}): ret={}", grant, request, ret);

            return ret;
        }

        private boolean isResourceMatch(RangerAccessRequest request) {
            boolean ret = CollectionUtils.isEmpty(grant.getResources()); // match all resources

            if (!ret) {
                for (RangerPolicyResourceMatcher matcher : resourceMatchers) {
                    ret = matcher.isMatch(request.getResource(), request.getContext());

                    if (ret) {
                        break;
                    }
                }
            }

            LOG.debug("isResourceMatch(grant={}, request={}): ret={}", grant, request, ret);

            return ret;
        }
    }

    private static class RangerGrantorAccessRequest extends RangerAccessRequestWrapper implements AutoCloseable {
        private final String      user;
        private final Set<String> userGroups;
        private final Set<String> userRoles;
        private final String      savedUser;
        private final Set<String> savedUserRoles;

        public RangerGrantorAccessRequest(RangerAccessRequest request, RangerPrincipal grantor) {
            super(request, request.getAccessType());

            user       = grantor.getType() == RangerPrincipal.PrincipalType.USER ? grantor.getName() : "";
            userGroups = grantor.getType() == RangerPrincipal.PrincipalType.GROUP ? Collections.singleton(grantor.getName()) : Collections.emptySet();
            userRoles  = grantor.getType() == RangerPrincipal.PrincipalType.ROLE ? Collections.singleton(grantor.getName()) : Collections.emptySet();

            savedUser      = RangerAccessRequestUtil.getCurrentUserFromContext(request.getContext());
            savedUserRoles = RangerAccessRequestUtil.getCurrentUserRolesFromContext(request.getContext());

            RangerAccessRequestUtil.setCurrentUserInContext(getContext(), user);
            RangerAccessRequestUtil.setCurrentUserRolesInContext(getContext(), userRoles);
        }

        @Override
        public void close() {
            RangerAccessRequestUtil.setCurrentUserInContext(getContext(), savedUser);
            RangerAccessRequestUtil.setCurrentUserRolesInContext(getContext(), savedUserRoles);
        }

        @Override
        public String getUser() {
            return user;
        }

        @Override
        public Set<String> getUserGroups() {
            return userGroups;
        }

        @Override
        public Set<String> getUserRoles() {
            return userRoles;
        }

        @Override
        public RangerInlinePolicy getInlinePolicy() {
            return null;
        }
    }
}
