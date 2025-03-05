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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemRowFilterInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPrincipal.PrincipalType;
import org.apache.ranger.plugin.store.PList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerGds {
    public enum GdsPermission { NONE, LIST, VIEW, AUDIT, POLICY_ADMIN, ADMIN }

    public enum GdsShareStatus { NONE, REQUESTED, GRANTED, DENIED, ACTIVE }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerGdsBaseModelObject extends RangerBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String              description;
        private Map<String, String> options;
        private Map<String, String> additionalInfo;

        public RangerGdsBaseModelObject() {
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public Map<String, String> getOptions() {
            return options;
        }

        public void setOptions(Map<String, String> options) {
            this.options = options;
        }

        public Map<String, String> getAdditionalInfo() {
            return additionalInfo;
        }

        public void setAdditionalInfo(Map<String, String> additionalInfo) {
            this.additionalInfo = additionalInfo;
        }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            super.toString(sb);

            sb.append("description={").append(description).append("} ")
                    .append("options={").append(options).append("} ")
                    .append("additionalInfo={").append(additionalInfo).append("} ");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerDataset extends RangerGdsBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String                 name;
        private RangerGdsObjectACL     acl;
        private RangerValiditySchedule validitySchedule;
        private String                 termsOfUse;
        private List<String>           labels;
        private List<String>           keywords;

        public RangerDataset() {
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public RangerGdsObjectACL getAcl() {
            return acl;
        }

        public void setAcl(RangerGdsObjectACL acl) {
            this.acl = acl;
        }

        public RangerValiditySchedule getValiditySchedule() {
            return validitySchedule;
        }

        public void setValiditySchedule(RangerValiditySchedule validitySchedule) {
            this.validitySchedule = validitySchedule;
        }

        public String getTermsOfUse() {
            return termsOfUse;
        }

        public void setTermsOfUse(String termsOfUse) {
            this.termsOfUse = termsOfUse;
        }

        public List<String> getLabels() {
            return labels;
        }

        public void setLabels(List<String> labels) {
            this.labels = labels;
        }

        public List<String> getKeywords() {
            return keywords;
        }

        public void setKeywords(List<String> keywords) {
            this.keywords = keywords;
        }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            sb.append("RangerDataset={");

            super.toString(sb);

            sb.append("name={").append(name).append("} ")
                    .append("acl={").append(acl).append("} ")
                    .append("validitySchedule={").append(validitySchedule).append("} ")
                    .append("termsOfUse={").append(termsOfUse).append("} ")
                    .append("labels={").append(labels).append("} ")
                    .append("keywords={").append(keywords).append("} ")
                    .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerProject extends RangerGdsBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String                 name;
        private RangerGdsObjectACL     acl;
        private RangerValiditySchedule validitySchedule;
        private String                 termsOfUse;

        public RangerProject() {
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public RangerGdsObjectACL getAcl() {
            return acl;
        }

        public void setAcl(RangerGdsObjectACL acl) {
            this.acl = acl;
        }

        public RangerValiditySchedule getValiditySchedule() {
            return validitySchedule;
        }

        public void setValiditySchedule(RangerValiditySchedule validitySchedule) {
            this.validitySchedule = validitySchedule;
        }

        public String getTermsOfUse() {
            return termsOfUse;
        }

        public void setTermsOfUse(String termsOfUse) {
            this.termsOfUse = termsOfUse;
        }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            sb.append("RangerProject={");

            super.toString(sb);

            sb.append("name={").append(name).append("} ")
                    .append("acl={").append(acl).append("} ")
                    .append("validitySchedule={").append(validitySchedule).append("} ")
                    .append("termsOfUse={").append(termsOfUse).append("} ")
                    .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerDataShare extends RangerGdsBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String                  name;
        private RangerGdsObjectACL      acl;
        private String                  service;
        private String                  zone;
        private String                  conditionExpr;
        private Set<String>             defaultAccessTypes;
        private List<RangerGdsMaskInfo> defaultTagMasks;
        private String                  termsOfUse;

        public RangerDataShare() {
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public RangerGdsObjectACL getAcl() {
            return acl;
        }

        public void setAcl(RangerGdsObjectACL acl) {
            this.acl = acl;
        }

        public String getService() {
            return service;
        }

        public void setService(String service) {
            this.service = service;
        }

        public String getZone() {
            return zone;
        }

        public void setZone(String zone) {
            this.zone = zone;
        }

        public String getConditionExpr() {
            return conditionExpr;
        }

        public void setConditionExpr(String conditionExpr) {
            this.conditionExpr = conditionExpr;
        }

        public Set<String> getDefaultAccessTypes() {
            return defaultAccessTypes;
        }

        public void setDefaultAccessTypes(Set<String> defaultAccessTypes) {
            this.defaultAccessTypes = defaultAccessTypes;
        }

        public List<RangerGdsMaskInfo> getDefaultTagMasks() {
            return defaultTagMasks;
        }

        public void setDefaultTagMasks(List<RangerGdsMaskInfo> defaultTagMasks) {
            this.defaultTagMasks = defaultTagMasks;
        }

        public String getTermsOfUse() {
            return termsOfUse;
        }

        public void setTermsOfUse(String termsOfUse) {
            this.termsOfUse = termsOfUse;
        }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            sb.append("RangerDataShare={");

            super.toString(sb);

            sb.append("name={").append(name).append("} ")
                    .append("acl={").append(acl).append("} ")
                    .append("service={").append(service).append("} ")
                    .append("zone={").append(zone).append("} ")
                    .append("conditionExpr={").append(conditionExpr).append("} ")
                    .append("defaultAccessTypes={").append(defaultAccessTypes).append("} ")
                    .append("defaultTagMasks={").append(defaultTagMasks).append("} ")
                    .append("termsOfUse={").append(termsOfUse).append("} ")
                    .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerSharedResource extends RangerGdsBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String                            name;
        private Long                              dataShareId;
        private Map<String, RangerPolicyResource> resource;
        private RangerPolicyResource              subResource;
        private String                            subResourceType;
        private String                            conditionExpr;
        private Set<String>                       accessTypes;
        private RangerPolicyItemRowFilterInfo     rowFilter;
        private List<RangerGdsMaskInfo>           subResourceMasks;
        private Set<String>                       profiles;

        public RangerSharedResource() {
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Long getDataShareId() {
            return dataShareId;
        }

        public void setDataShareId(Long dataShareId) {
            this.dataShareId = dataShareId;
        }

        public Map<String, RangerPolicyResource> getResource() {
            return resource;
        }

        public void setResource(Map<String, RangerPolicyResource> resource) {
            this.resource = resource;
        }

        public RangerPolicyResource getSubResource() {
            return subResource;
        }

        public void setSubResource(RangerPolicyResource subResource) {
            this.subResource = subResource;
        }

        public String getSubResourceType() {
            return subResourceType;
        }

        public void setSubResourceType(String subResourceType) {
            this.subResourceType = subResourceType;
        }

        public String getConditionExpr() {
            return conditionExpr;
        }

        public void setConditionExpr(String conditionExpr) {
            this.conditionExpr = conditionExpr;
        }

        public Set<String> getAccessTypes() {
            return accessTypes;
        }

        public void setAccessTypes(Set<String> accessTypes) {
            this.accessTypes = accessTypes;
        }

        public RangerPolicyItemRowFilterInfo getRowFilter() {
            return rowFilter;
        }

        public void setRowFilter(RangerPolicyItemRowFilterInfo rowFilter) {
            this.rowFilter = rowFilter;
        }

        public List<RangerGdsMaskInfo> getSubResourceMasks() {
            return subResourceMasks;
        }

        public void setSubResourceMasks(List<RangerGdsMaskInfo> subResourceMasks) {
            this.subResourceMasks = subResourceMasks;
        }

        public Set<String> getProfiles() {
            return profiles;
        }

        public void setProfiles(Set<String> profiles) {
            this.profiles = profiles;
        }

        public StringBuilder toString(StringBuilder sb) {
            sb.append("RangerSharedResource={");

            super.toString(sb);

            sb.append("name").append(name).append("} ")
                    .append("dataShareId={").append(dataShareId).append("} ")
                    .append("resource={").append(resource).append("} ")
                    .append("subResource={").append(subResource).append("} ")
                    .append("subResourceType={").append(subResourceType).append("} ")
                    .append("conditionExpr={").append(conditionExpr).append("} ")
                    .append("accessTypes={").append(accessTypes).append("} ")
                    .append("rowFilterInfo={").append(rowFilter).append("} ")
                    .append("subResourceMasks={").append(subResourceMasks).append("} ")
                    .append("profiles={").append(profiles).append("} ")
                    .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerGdsMaskInfo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private List<String>                 values;
        private RangerPolicyItemDataMaskInfo maskInfo;

        public List<String> getValues() {
            return values;
        }

        public void setValues(List<String> values) {
            this.values = values;
        }

        public RangerPolicyItemDataMaskInfo getMaskInfo() {
            return maskInfo;
        }

        public void setMaskInfo(RangerPolicyItemDataMaskInfo maskInfo) {
            this.maskInfo = maskInfo;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            sb.append("RangerGdsMaskInfo={")
                    .append("values=").append(values).append(" ")
                    .append("maskInfo=").append(maskInfo)
                    .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerDataShareInDataset extends RangerGdsBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private Long                   dataShareId;
        private Long                   datasetId;
        private GdsShareStatus         status;
        private RangerValiditySchedule validitySchedule;
        private Set<String>            profiles;
        private String                 approver;

        public RangerDataShareInDataset() {
        }

        public Long getDataShareId() {
            return dataShareId;
        }

        public void setDataShareId(Long dataShareId) {
            this.dataShareId = dataShareId;
        }

        public Long getDatasetId() {
            return datasetId;
        }

        public void setDatasetId(Long datasetId) {
            this.datasetId = datasetId;
        }

        public GdsShareStatus getStatus() {
            return status;
        }

        public void setStatus(GdsShareStatus status) {
            this.status = status;
        }

        public RangerValiditySchedule getValiditySchedule() {
            return validitySchedule;
        }

        public void setValiditySchedule(RangerValiditySchedule validitySchedule) {
            this.validitySchedule = validitySchedule;
        }

        public Set<String> getProfiles() {
            return profiles;
        }

        public void setProfiles(Set<String> profiles) {
            this.profiles = profiles;
        }

        public String getApprover() {
            return approver;
        }

        public void setApprover(String approver) {
            this.approver = approver;
        }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            sb.append("RangerDataShareInDataset={");

            super.toString(sb);

            sb.append("dataShareId={").append(dataShareId).append("} ")
                    .append("datasetId={").append(datasetId).append("} ")
                    .append("status={").append(status).append("} ")
                    .append("validitySchedule={").append(validitySchedule).append("} ")
                    .append("profiles={").append(profiles).append("} ")
                    .append("approver={").append(approver).append("} ")
                    .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerDatasetInProject extends RangerGdsBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private Long                   datasetId;
        private Long                   projectId;
        private GdsShareStatus         status;
        private RangerValiditySchedule validitySchedule;
        private Set<String>            profiles;
        private String                 approver;

        public RangerDatasetInProject() {
        }

        public Long getDatasetId() {
            return datasetId;
        }

        public void setDatasetId(Long datasetId) {
            this.datasetId = datasetId;
        }

        public Long getProjectId() {
            return projectId;
        }

        public void setProjectId(Long projectId) {
            this.projectId = projectId;
        }

        public GdsShareStatus getStatus() {
            return status;
        }

        public void setStatus(GdsShareStatus status) {
            this.status = status;
        }

        public RangerValiditySchedule getValiditySchedule() {
            return validitySchedule;
        }

        public void setValiditySchedule(RangerValiditySchedule validitySchedule) {
            this.validitySchedule = validitySchedule;
        }

        public Set<String> getProfiles() {
            return profiles;
        }

        public void setProfiles(Set<String> profiles) {
            this.profiles = profiles;
        }

        public String getApprover() {
            return approver;
        }

        public void setApprover(String approver) {
            this.approver = approver;
        }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            sb.append("RangerDatasetInProject={");

            super.toString(sb);

            sb.append("datasetGuid={").append(datasetId).append("} ")
                    .append("projectGuid={").append(projectId).append("} ")
                    .append("status={").append(status).append("} ")
                    .append("validitySchedule={").append(validitySchedule).append("} ")
                    .append("profiles={").append(profiles).append("} ")
                    .append("approver={").append(approver).append("} ")
                    .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerGdsObjectACL implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private Map<String, GdsPermission> users;
        private Map<String, GdsPermission> groups;
        private Map<String, GdsPermission> roles;

        public RangerGdsObjectACL() {
        }

        public Map<String, GdsPermission> getUsers() {
            return users;
        }

        public void setUsers(Map<String, GdsPermission> users) {
            this.users = users;
        }

        public Map<String, GdsPermission> getGroups() {
            return groups;
        }

        public void setGroups(Map<String, GdsPermission> groups) {
            this.groups = groups;
        }

        public Map<String, GdsPermission> getRoles() {
            return roles;
        }

        public void setRoles(Map<String, GdsPermission> roles) {
            this.roles = roles;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            sb.append("RangerGdsObjectACL={");

            sb.append("users={").append(users).append("} ")
                    .append("groups={").append(groups).append("} ")
                    .append("roles={").append(roles).append("} ")
                    .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DatasetSummary extends RangerBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String                          name;
        private String                          description;
        private GdsPermission                   permissionForCaller;
        private Map<PrincipalType, Integer>     principalsCount;
        private Map<PrincipalType, Integer>     aclPrincipalsCount;
        private Long                            projectsCount;
        private Long                            totalResourceCount;
        private List<DataShareInDatasetSummary> dataShares;
        private RangerValiditySchedule          validitySchedule;
        private List<String>                    labels;
        private List<String>                    keywords;

        public DatasetSummary() {
            super();
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public GdsPermission getPermissionForCaller() {
            return permissionForCaller;
        }

        public void setPermissionForCaller(GdsPermission permissionForCaller) {
            this.permissionForCaller = permissionForCaller;
        }

        public Map<PrincipalType, Integer> getPrincipalsCount() {
            return principalsCount;
        }

        public void setPrincipalsCount(Map<PrincipalType, Integer> principalsCount) {
            this.principalsCount = principalsCount;
        }

        public Long getProjectsCount() {
            return projectsCount;
        }

        public void setProjectsCount(Long projectsCount) {
            this.projectsCount = projectsCount;
        }

        public Long getTotalResourceCount() {
            return totalResourceCount;
        }

        public void setTotalResourceCount(Long totalResourceCount) {
            this.totalResourceCount = totalResourceCount;
        }

        public List<DataShareInDatasetSummary> getDataShares() {
            return dataShares;
        }

        public void setDataShares(List<DataShareInDatasetSummary> dataShares) {
            this.dataShares = dataShares;
        }

        public Map<PrincipalType, Integer> getAclPrincipalsCount() {
            return aclPrincipalsCount;
        }

        public void setAclPrincipalsCount(Map<PrincipalType, Integer> aclPrincipalsCount) {
            this.aclPrincipalsCount = aclPrincipalsCount;
        }

        public RangerValiditySchedule getValiditySchedule() {
            return validitySchedule;
        }

        public void setValiditySchedule(RangerValiditySchedule validitySchedule) {
            this.validitySchedule = validitySchedule;
        }

        public List<String> getLabels() {
            return labels;
        }

        public void setLabels(List<String> labels) {
            this.labels = labels;
        }

        public List<String> getKeywords() {
            return keywords;
        }

        public void setKeywords(List<String> keywords) {
            this.keywords = keywords;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            sb.append("DatasetSummary={");

            super.toString(sb);

            sb.append("name={").append(name).append("} ")
                    .append("description={").append(description).append("} ")
                    .append("permissionForCaller={").append(permissionForCaller).append("} ")
                    .append("principalsCount={").append(principalsCount).append("} ")
                    .append("projectsCount={").append(projectsCount).append("} ")
                    .append("aclPrincipalsCount={").append(aclPrincipalsCount).append("} ")
                    .append("totalResourceCount={").append(totalResourceCount).append("} ")
                    .append("dataShares={").append(dataShares).append("} ")
                    .append("validitySchedule={").append(validitySchedule).append("} ")
                    .append("labels={").append(labels).append("} ")
                    .append("keywords={").append(keywords).append("} ")
                    .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DatasetsSummary extends PList<DatasetSummary> {
        private static final long serialVersionUID = 1L;
        private Map<String, Map<String, Integer>> additionalInfo;

        public DatasetsSummary() {
            super();
        }

        public DatasetsSummary(PList<DatasetSummary> datasetSummary, Map<String, Map<String, Integer>> additionalInfo) {
            super(datasetSummary);
            this.additionalInfo = (additionalInfo != null) ? additionalInfo : Collections.emptyMap();
        }

        public Map<String, Map<String, Integer>> getAdditionalInfo() {
            return additionalInfo;
        }

        public void setAdditionalInfo(Map<String, Map<String, Integer>> additionalInfo) {
            this.additionalInfo = additionalInfo;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            sb.append("DatasetsSummary={")
                    .append("list={").append(this.list).append("} ")
                    .append("additionalInfo={").append(additionalInfo).append("} ")
                    .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DataShareSummary extends RangerBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String                          name;
        private String                          description;
        private GdsPermission                   permissionForCaller;
        private Long                            resourceCount;
        private Long                            serviceId;
        private String                          serviceName;
        private String                          serviceType;
        private Long                            zoneId;
        private String                          zoneName;
        private List<DataShareInDatasetSummary> datasets;

        public DataShareSummary() {
            super();
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public GdsPermission getPermissionForCaller() {
            return permissionForCaller;
        }

        public void setPermissionForCaller(GdsPermission permissionForCaller) {
            this.permissionForCaller = permissionForCaller;
        }

        public Long getResourceCount() {
            return resourceCount;
        }

        public void setResourceCount(Long resourceCount) {
            this.resourceCount = resourceCount;
        }

        public Long getServiceId() {
            return serviceId;
        }

        public void setServiceId(Long serviceId) {
            this.serviceId = serviceId;
        }

        public String getServiceName() {
            return serviceName;
        }

        public void setServiceName(String serviceName) {
            this.serviceName = serviceName;
        }

        public String getServiceType() {
            return serviceType;
        }

        public void setServiceType(String serviceType) {
            this.serviceType = serviceType;
        }

        public Long getZoneId() {
            return zoneId;
        }

        public void setZoneId(Long zoneId) {
            this.zoneId = zoneId;
        }

        public String getZoneName() {
            return zoneName;
        }

        public void setZoneName(String zoneName) {
            this.zoneName = zoneName;
        }

        public List<DataShareInDatasetSummary> getDatasets() {
            return datasets;
        }

        public void setDatasets(List<DataShareInDatasetSummary> datasets) {
            this.datasets = datasets;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            sb.append("DataShareSummary={");

            super.toString(sb);

            sb.append("name={").append(name).append("} ")
                    .append("description={").append(description).append("} ")
                    .append("permissionForCaller={").append(permissionForCaller).append("} ")
                    .append("resourceCount={").append(resourceCount).append("} ")
                    .append("serviceId={").append(serviceId).append("} ")
                    .append("serviceName={").append(serviceName).append("} ")
                    .append("serviceType={").append(serviceType).append("} ")
                    .append("zoneName={").append(zoneName).append("} ")
                    .append("zoneId={").append(zoneId).append("} ")
                    .append("datasets={").append(datasets).append("} ")
                    .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DataShareInDatasetSummary extends RangerBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private Long           datasetId;
        private String         datasetName;
        private Long           dataShareId;
        private String         dataShareName;
        private Long           serviceId;
        private String         serviceName;
        private Long           zoneId;
        private String         zoneName;
        private Long           resourceCount;
        private GdsShareStatus shareStatus;
        private String         approver;

        public DataShareInDatasetSummary() {
            super();
        }

        public String getDatasetName() {
            return datasetName;
        }

        public void setDatasetName(String datasetName) {
            this.datasetName = datasetName;
        }

        public Long getDatasetId() {
            return datasetId;
        }

        public void setDatasetId(Long datasetId) {
            this.datasetId = datasetId;
        }

        public Long getDataShareId() {
            return dataShareId;
        }

        public void setDataShareId(Long dataShareId) {
            this.dataShareId = dataShareId;
        }

        public String getDataShareName() {
            return dataShareName;
        }

        public void setDataShareName(String dataShareName) {
            this.dataShareName = dataShareName;
        }

        public Long getServiceId() {
            return serviceId;
        }

        public void setServiceId(Long serviceId) {
            this.serviceId = serviceId;
        }

        public String getServiceName() {
            return serviceName;
        }

        public void setServiceName(String serviceName) {
            this.serviceName = serviceName;
        }

        public Long getZoneId() {
            return zoneId;
        }

        public void setZoneId(Long zoneId) {
            this.zoneId = zoneId;
        }

        public String getZoneName() {
            return zoneName;
        }

        public void setZoneName(String zoneName) {
            this.zoneName = zoneName;
        }

        public Long getResourceCount() {
            return resourceCount;
        }

        public void setResourceCount(Long resourceCount) {
            this.resourceCount = resourceCount;
        }

        public GdsShareStatus getShareStatus() {
            return shareStatus;
        }

        public void setShareStatus(GdsShareStatus shareStatus) {
            this.shareStatus = shareStatus;
        }

        public String getApprover() {
            return approver;
        }

        public void setApprover(String approver) {
            this.approver = approver;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            sb.append("DataShareInDatasetSummary={");

            super.toString(sb);

            sb.append("name={").append(datasetName).append("} ")
                    .append("datasetId={").append(datasetId).append("} ")
                    .append("datasetName={").append(datasetName).append("} ")
                    .append("dataShareId={").append(dataShareId).append("} ")
                    .append("dataShareName={").append(dataShareName).append("} ")
                    .append("serviceId={").append(serviceId).append("} ")
                    .append("serviceName={").append(serviceName).append("} ")
                    .append("zoneId={").append(zoneId).append("} ")
                    .append("zoneName={").append(zoneName).append("} ")
                    .append("resourceCount={").append(resourceCount).append("} ")
                    .append("shareStatus={").append(shareStatus).append("} ")
                    .append("approver={").append(approver).append("} ")
                    .append("}");

            return sb;
        }
    }
}
