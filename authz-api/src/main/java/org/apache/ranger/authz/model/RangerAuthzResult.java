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

package org.apache.ranger.authz.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerAuthzResult {
    public enum AccessDecision { ALLOW, DENY, NOT_DETERMINED, PARTIAL }

    private String                        requestId;
    private AccessDecision                decision;
    private Map<String, PermissionResult> permissions;

    public RangerAuthzResult() {
    }

    public RangerAuthzResult(String requestId) {
        this(requestId, null);
    }

    public RangerAuthzResult(String requestId, Map<String, PermissionResult> permissions) {
        this.requestId   = requestId;
        this.permissions = permissions;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public AccessDecision getDecision() {
        return decision;
    }

    public void setDecision(AccessDecision decision) {
        this.decision = decision;
    }

    public Map<String, PermissionResult> getPermissions() {
        return permissions;
    }

    public void setPermissions(Map<String, PermissionResult> permissions) {
        this.permissions = permissions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, decision, permissions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RangerAuthzResult that = (RangerAuthzResult) o;

        return Objects.equals(requestId, that.requestId) &&
                Objects.equals(decision, that.decision) &&
                Objects.equals(permissions, that.permissions);
    }

    @Override
    public String toString() {
        return "RangerAuthzResult{" +
                "requestId='" + requestId + '\'' +
                ", decision=" + decision +
                ", permissions=" + permissions +
                '}';
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PermissionResult {
        private String                        permission;
        private AccessResult                  access;
        private DataMaskResult                dataMask;
        private RowFilterResult               rowFilter;
        private Map<String, PermissionResult> subResources;
        private Map<String, Object>           additionalInfo;

        public PermissionResult() {
        }

        public PermissionResult(String permission) {
            this(permission, null, null);
        }

        public PermissionResult(String permission, AccessResult access) {
            this(permission, access, null);
        }

        public PermissionResult(String permission, AccessResult access, Map<String, Object> additionalInfo) {
            this.permission     = permission;
            this.access         = access;
            this.additionalInfo = additionalInfo;
        }

        // Getters and Setters
        public String getPermission() {
            return permission;
        }

        public void setPermission(String permission) {
            this.permission = permission;
        }

        public AccessResult getAccess() {
            return access;
        }

        public void setAccess(AccessResult access) {
            this.access = access;
        }

        public DataMaskResult getDataMask() {
            return dataMask;
        }

        public void setDataMask(DataMaskResult dataMask) {
            this.dataMask = dataMask;
        }

        public RowFilterResult getRowFilter() {
            return rowFilter;
        }

        public void setRowFilter(RowFilterResult rowFilter) {
            this.rowFilter = rowFilter;
        }

        public Map<String, PermissionResult> getSubResources() {
            return subResources;
        }

        public void setSubResources(Map<String, PermissionResult> subResources) {
            this.subResources = subResources;
        }

        public PermissionResult getdSubResourceResult(String resourceName) {
            Map<String, PermissionResult> subResources = getSubResources();

            return subResources != null ? subResources.get(resourceName) : null;
        }

        public void addSubResourceResult(String resourceName, PermissionResult result) {
            if (subResources == null) {
                subResources = new HashMap<>();
            }

            subResources.put(resourceName, result);
        }

        public Map<String, Object> getAdditionalInfo() {
            return additionalInfo;
        }

        public void setAdditionalInfo(Map<String, Object> additionalInfo) {
            this.additionalInfo = additionalInfo;
        }

        @Override
        public int hashCode() {
            return Objects.hash(permission, access, dataMask, rowFilter, subResources, additionalInfo);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PermissionResult that = (PermissionResult) o;

            return Objects.equals(permission, that.permission) &&
                    Objects.equals(access, that.access) &&
                    Objects.equals(dataMask, that.dataMask) &&
                    Objects.equals(rowFilter, that.rowFilter) &&
                    Objects.equals(subResources, that.subResources) &&
                    Objects.equals(additionalInfo, that.additionalInfo);
        }

        @Override
        public String toString() {
            return "PermissionResult{" +
                    "permission='" + permission + '\'' +
                    ", access=" + access +
                    ", dataMask=" + dataMask +
                    ", rowFilter=" + rowFilter +
                    ", subResources=" + subResources +
                    ", additionalInfo=" + additionalInfo +
                    '}';
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AccessResult {
        AccessDecision decision;
        PolicyInfo     policy;

        public AccessResult() {
        }

        public AccessResult(AccessDecision decision, PolicyInfo policy) {
            this.decision = decision;
            this.policy   = policy;
        }

        public AccessDecision getDecision() {
            return decision;
        }

        public void setDecision(AccessDecision decision) {
            this.decision = decision;
        }

        public PolicyInfo getPolicy() {
            return policy;
        }

        public void setPolicy(PolicyInfo policy) {
            this.policy = policy;
        }

        @Override
        public int hashCode() {
            return Objects.hash(decision, policy);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            }

            AccessResult that = (AccessResult) o;

            return Objects.equals(decision, that.decision) &&
                    Objects.equals(policy, that.policy);
        }

        @Override
        public String toString() {
            return "AccessResult{" +
                    "decision=" + decision +
                    ", policy=" + policy +
                    '}';
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DataMaskResult {
        private String     maskType;
        private String     maskedValue;
        private PolicyInfo policy;

        public DataMaskResult() {
        }

        public DataMaskResult(String maskType, String maskedValue, PolicyInfo policy) {
            this.maskType    = maskType;
            this.maskedValue = maskedValue;
            this.policy      = policy;
        }

        public String getMaskType() {
            return maskType;
        }

        public void setMaskType(String maskType) {
            this.maskType = maskType;
        }

        public String getMaskedValue() {
            return maskedValue;
        }

        public void setMaskedValue(String maskedValue) {
            this.maskedValue = maskedValue;
        }

        public PolicyInfo getPolicy() {
            return policy;
        }

        public void setPolicy(PolicyInfo policy) {
            this.policy = policy;
        }

        @Override
        public int hashCode() {
            return Objects.hash(maskType, maskedValue, policy);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DataMaskResult that = (DataMaskResult) o;

            return Objects.equals(maskType, that.maskType) &&
                    Objects.equals(maskedValue, that.maskedValue) &&
                    Objects.equals(policy, that.policy);
        }

        @Override
        public String toString() {
            return "DataMaskResult{" +
                    "maskType='" + maskType + '\'' +
                    ", maskedValue='" + maskedValue + '\'' +
                    ", policy='" + policy + '\'' +
                    '}';
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RowFilterResult {
        private String     filterExpr;
        private PolicyInfo policy;

        public RowFilterResult() {
        }

        public RowFilterResult(String filterExpr, PolicyInfo policy) {
            this.filterExpr = filterExpr;
            this.policy     = policy;
        }

        public String getFilterExpr() {
            return filterExpr;
        }

        public void setFilterExpr(String filterExpr) {
            this.filterExpr = filterExpr;
        }

        public PolicyInfo getPolicy() {
            return policy;
        }

        public void setPolicy(PolicyInfo policy) {
            this.policy = policy;
        }

        @Override
        public int hashCode() {
            return Objects.hash(filterExpr, policy);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            }

            RowFilterResult that = (RowFilterResult) o;

            return Objects.equals(filterExpr, that.filterExpr) &&
                    Objects.equals(policy, that.policy);
        }

        @Override
        public String toString() {
            return "RowFilterResult{" +
                    "filterExpr='" + filterExpr + '\'' +
                    ", policy=" + policy +
                    '}';
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PolicyInfo {
        private Long id;
        private Long version;

        public PolicyInfo() {
        }

        public PolicyInfo(Long id, Long version) {
            this.id      = id;
            this.version = version;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Long getVersion() {
            return version;
        }

        public void setVersion(Long version) {
            this.version = version;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, version);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PolicyInfo that = (PolicyInfo) o;

            return Objects.equals(id, that.id) &&
                    Objects.equals(version, that.version);
        }

        @Override
        public String toString() {
            return "PolicyInfo{" +
                    "id='" + id + '\'' +
                    ", version=" + version +
                    '}';
        }
    }
}
