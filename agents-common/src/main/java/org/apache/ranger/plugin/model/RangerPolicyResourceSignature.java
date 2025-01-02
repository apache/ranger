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

package org.apache.ranger.plugin.model;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.plugin.model.RangerGds.RangerSharedResource;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class RangerPolicyResourceSignature {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPolicyResourceSignature.class);

    static final int                           SignatureVersion       = 1;
    static final RangerPolicyResourceSignature EmptyResourceSignature = new RangerPolicyResourceSignature((RangerPolicy) null);

    private final String       strValue;
    private final String       hashValue;
    private final RangerPolicy policy;

    public RangerPolicyResourceSignature(RangerPolicy policy) {
        this.policy = policy;

        PolicySerializer serializer = new PolicySerializer(this.policy);

        strValue = serializer.toString();

        if (RangerAdminConfig.getInstance().isFipsEnabled()) {
            hashValue = DigestUtils.sha512Hex(strValue);
        } else {
            hashValue = DigestUtils.sha256Hex(strValue);
        }
    }

    public RangerPolicyResourceSignature(RangerSharedResource resource) {
        this(toSignatureString(resource));
    }

    public RangerPolicyResourceSignature(Map<String, RangerPolicyResource> resources) {
        this(toSignatureString(resources));
    }

    /**
     * Only added for testability.  Do not make public
     *
     * @param string
     */
    RangerPolicyResourceSignature(String string) {
        policy = null;

        if (string == null) {
            strValue = "";
        } else {
            strValue = string;
        }

        if (RangerAdminConfig.getInstance().isFipsEnabled()) {
            hashValue = DigestUtils.sha384Hex(strValue);
        } else {
            hashValue = DigestUtils.sha256Hex(strValue);
        }
    }

    // alternate to constructor that takes Map<String, List<String>>
    public static RangerPolicyResourceSignature from(Map<String, List<String>> resources) {
        return new RangerPolicyResourceSignature(toPolicyResources(resources));
    }

    public static String toSignatureString(Map<String, RangerPolicyResource> resource) {
        Map<String, ResourceSerializer> resources = new TreeMap<>();

        for (Map.Entry<String, RangerPolicyResource> entry : resource.entrySet()) {
            String             resourceName = entry.getKey();
            ResourceSerializer resourceView = new ResourceSerializer(entry.getValue());

            resources.put(resourceName, resourceView);
        }

        return resources.toString();
    }

    public static String toSignatureString(Map<String, RangerPolicyResource> resource, List<Map<String, RangerPolicyResource>> additionalResources) {
        String ret = toSignatureString(resource);

        if (additionalResources != null && !additionalResources.isEmpty()) {
            List<String> signatures = new ArrayList<>(additionalResources.size() + 1);

            signatures.add(ret);

            for (Map<String, RangerPolicyResource> additionalResource : additionalResources) {
                signatures.add(toSignatureString(additionalResource));
            }

            Collections.sort(signatures);

            ret = signatures.toString();
        }

        return ret;
    }

    public static String toSignatureString(RangerSharedResource resource) {
        final Map<String, RangerPolicyResource> policyResource;

        if (StringUtils.isNotBlank(resource.getSubResourceType()) && resource.getSubResource() != null && CollectionUtils.isNotEmpty(resource.getSubResource().getValues())) {
            policyResource = new HashMap<>(resource.getResource());

            policyResource.put(resource.getSubResourceType(), resource.getSubResource());
        } else {
            policyResource = resource.getResource();
        }

        String signature = toSignatureString(policyResource);

        if (StringUtils.isNotEmpty(resource.getConditionExpr())) {
            signature += resource.getConditionExpr();
        }

        return String.format("{version=%d,ret=%s}", SignatureVersion, signature);
    }

    public String getSignature() {
        return hashValue;
    }

    @Override
    public int hashCode() {
        // we assume no collision
        return Objects.hashCode(hashValue);
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof RangerPolicyResourceSignature)) {
            return false;
        }

        RangerPolicyResourceSignature that = (RangerPolicyResourceSignature) object;

        return Objects.equals(this.hashValue, that.hashValue);
    }

    @Override
    public String toString() {
        return String.format("%s: %s", hashValue, strValue);
    }

    String asString() {
        return strValue;
    }

    private static Map<String, RangerPolicyResource> toPolicyResources(Map<String, List<String>> resources) {
        Map<String, RangerPolicyResource> ret = new TreeMap<>();

        for (Map.Entry<String, List<String>> entry : resources.entrySet()) {
            ret.put(entry.getKey(), new RangerPolicyResource(entry.getValue(), false, false));
        }

        return ret;
    }

    static class PolicySerializer {
        final RangerPolicy policy;

        PolicySerializer(RangerPolicy policy) {
            this.policy = policy;
        }

        @Override
        public String toString() {
            // invalid/empty policy gets a deterministic signature as if it had an empty resource string
            if (!isPolicyValidForResourceSignatureComputation()) {
                return "";
            }

            int type = RangerPolicy.POLICY_TYPE_ACCESS;

            if (policy.getPolicyType() != null) {
                type = policy.getPolicyType();
            }

            String resource = toSignatureString(policy.getResources(), policy.getAdditionalResources());

            if (CollectionUtils.isNotEmpty(policy.getValiditySchedules())) {
                resource += policy.getValiditySchedules().toString();
            }

            if (policy.getPolicyPriority() != null && policy.getPolicyPriority() != RangerPolicy.POLICY_PRIORITY_NORMAL) {
                resource += policy.getPolicyPriority();
            }

            if (!StringUtils.isEmpty(policy.getZoneName())) {
                resource += policy.getZoneName();
            }

            if (policy.getConditions() != null) {
                CustomConditionSerialiser customConditionSerialiser = new CustomConditionSerialiser(policy.getConditions());

                resource += customConditionSerialiser.toString();
            }

            if (!policy.getIsEnabled()) {
                resource += policy.getGuid();
            }

            return String.format("{version=%d,type=%d,resource=%s}", SignatureVersion, type, resource);
        }

        boolean isPolicyValidForResourceSignatureComputation() {
            LOG.debug("==> RangerPolicyResourceSignature.isPolicyValidForResourceSignatureComputation({})", policy);

            boolean valid = false;

            if (policy == null) {
                LOG.debug("isPolicyValidForResourceSignatureComputation: policy was null!");
            } else if (policy.getResources() == null) {
                LOG.debug("isPolicyValidForResourceSignatureComputation: resources collection on policy was null!");
            } else if (policy.getResources().containsKey(null)) {
                LOG.debug("isPolicyValidForResourceSignatureComputation: resources collection has resource with null name!");
            } else if (!policy.getIsEnabled() && StringUtils.isEmpty(policy.getGuid())) {
                LOG.debug("isPolicyValidForResourceSignatureComputation: policy GUID is empty for a disabled policy!");
            } else {
                valid = true;
            }

            LOG.debug("<== RangerPolicyResourceSignature.isPolicyValidForResourceSignatureComputation({}): {}}", policy, valid);

            return valid;
        }
    }

    public static class ResourceSerializer {
        final RangerPolicyResource policyResource;

        public ResourceSerializer(RangerPolicyResource policyResource) {
            this.policyResource = policyResource;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder("{");

            if (policyResource != null) {
                builder.append("values=");

                if (policyResource.getValues() != null) {
                    List<String> values = new ArrayList<>(policyResource.getValues());

                    Collections.sort(values);
                    builder.append(values);
                }

                builder.append(",excludes=");

                if (policyResource.getIsExcludes() == null) { // null is same as false
                    builder.append(Boolean.FALSE);
                } else {
                    builder.append(policyResource.getIsExcludes());
                }

                builder.append(",recursive=");

                if (policyResource.getIsRecursive() == null) { // null is the same as false
                    builder.append(Boolean.FALSE);
                } else {
                    builder.append(policyResource.getIsRecursive());
                }
            }

            builder.append("}");

            return builder.toString();
        }
    }

    static class CustomConditionSerialiser {
        final List<RangerPolicy.RangerPolicyItemCondition> rangerPolicyConditions;

        CustomConditionSerialiser(List<RangerPolicyItemCondition> rangerPolicyConditions) {
            this.rangerPolicyConditions = rangerPolicyConditions;
        }

        @Override
        public String toString() {
            StringBuilder             builder      = new StringBuilder();
            Map<String, List<String>> conditionMap = new TreeMap<>();

            for (RangerPolicyItemCondition rangerPolicyCondition : rangerPolicyConditions) {
                if (rangerPolicyCondition.getType() != null) {
                    String       type   = rangerPolicyCondition.getType();
                    List<String> values = new ArrayList<>();

                    if (rangerPolicyCondition.getValues() != null) {
                        values.addAll(rangerPolicyCondition.getValues());

                        Collections.sort(values);
                    }

                    conditionMap.put(type, values);
                }
            }

            if (MapUtils.isNotEmpty(conditionMap)) {
                builder.append("{");
                builder.append("RangerPolicyConditions=");
                builder.append(conditionMap);
                builder.append("}");
            }

            return builder.toString();
        }
    }
}
