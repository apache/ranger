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

import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemRowFilterInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerGds {

    public enum GdsPermission { NONE, LIST, VIEW, AUDIT, POLICY_ADMIN, ADMIN }

    public enum GdsShareStatus { NONE, REQUESTED, GRANTED, ACCEPTED }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerGdsBaseModelObject extends RangerBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String              description;
        private Map<String, String> options;
        private Map<String, String> additionalInfo;


        public RangerGdsBaseModelObject() { }

        public String getDescription() { return description; }

        public void setDescription(String description) { this.description = description; }

        public Map<String, String> getOptions() { return options; }

        public void setOptions(Map<String, String> options) { this.options = options; }

        public Map<String, String> getAdditionalInfo() { return additionalInfo; }

        public void setAdditionalInfo(Map<String, String> additionalInfo) { this.additionalInfo = additionalInfo; }

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
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerDataset extends RangerGdsBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String                name;
        private List<RangerPrincipal> admins;
        private RangerGdsObjectACL    acl;
        private String                termsOfUse;

        public RangerDataset() { }

        public String getName() { return name; }

        public void setName(String name) { this.name = name; }

        public List<RangerPrincipal> getAdmins() { return admins; }

        public void setAdmins(List<RangerPrincipal> admins) { this.admins = admins; }

        public RangerGdsObjectACL getAcl() { return acl; }

        public void setAcl(RangerGdsObjectACL acl) { this.acl = acl; }

        public String getTermsOfUse() { return termsOfUse; }

        public void setTermsOfUse(String termsOfUse) { this.termsOfUse = termsOfUse; }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            sb.append("RangerDataset={");

            super.toString(sb);

            sb.append("name={").append(name).append("} ")
              .append("admin={").append(admins).append("} ")
              .append("acl={").append(acl).append("} ")
              .append("termsOfUse={").append(termsOfUse).append("} ")
              .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerProject extends RangerGdsBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String                name;
        private List<RangerPrincipal> admins;
        private RangerGdsObjectACL    acl;
        private String                termsOfUse;

        public RangerProject() { }

        public String getName() { return name; }

        public void setName(String name) { this.name = name; }

        public List<RangerPrincipal> getAdmins() { return admins; }

        public void setAdmins(List<RangerPrincipal> admins) { this.admins = admins; }

        public RangerGdsObjectACL getAcl() { return acl; }

        public void setAcl(RangerGdsObjectACL acl) { this.acl = acl; }

        public String getTermsOfUse() { return termsOfUse; }

        public void setTermsOfUse(String termsOfUse) { this.termsOfUse = termsOfUse; }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            sb.append("RangerProject={");

            super.toString(sb);

            sb.append("name={").append(name).append("} ")
              .append("admins={").append(admins).append("} ")
              .append("acl={").append(acl).append("} ")
              .append("termsOfUse={").append(termsOfUse).append("} ")
              .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerDataShare extends RangerGdsBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String                                    name;
        private List<RangerPrincipal>                     admins;
        private String                                    service;
        private String                                    zone;
        private String                                    conditionExpr;
        private Set<String>                               defaultAccessTypes;
        private Map<String, RangerPolicyItemDataMaskInfo> defaultMasks;
        private String                                    termsOfUse;

        public RangerDataShare() { }

        public String getName() { return name; }

        public void setName(String name) { this.name = name; }

        public List<RangerPrincipal> getAdmins() { return admins; }

        public void setAdmins(List<RangerPrincipal> admins) { this.admins = admins; }

        public String getService() { return service; }

        public void setService(String service) { this.service = service; }

        public String getZone() { return zone; }

        public void setZone(String zone) { this.zone = zone; }

        public String getConditionExpr() { return conditionExpr; }

        public void setConditionExpr(String conditionExpr) { this.conditionExpr = conditionExpr; }

        public Set<String> getDefaultAccessTypes() {
            return defaultAccessTypes;
        }

        public void setDefaultAccessTypes(Set<String> defaultAccessTypes) {
            this.defaultAccessTypes = defaultAccessTypes;
        }

        public Map<String, RangerPolicyItemDataMaskInfo> getDefaultMasks() {
            return defaultMasks;
        }

        public void setDefaultMasks(Map<String, RangerPolicyItemDataMaskInfo> defaultMasks) {
            this.defaultMasks = defaultMasks;
        }

        public String getTermsOfUse() { return termsOfUse; }

        public void setTermsOfUse(String termsOfUse) { this.termsOfUse = termsOfUse; }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            sb.append("RangerDataShare={");

            super.toString(sb);

            sb.append("name={").append(name).append("} ")
              .append("admins={").append(admins).append("} ")
              .append("service={").append(service).append("} ")
              .append("zone={").append(zone).append("} ")
              .append("conditionExpr={").append(conditionExpr).append("} ")
              .append("defaultAccessTypes={").append(defaultAccessTypes).append("} ")
              .append("defaultMasks={").append(defaultMasks).append("} ")
              .append("termsOfUse={").append(termsOfUse).append("} ")
              .append("}");

            return sb;
        }
    }

    public static class RangerSharedResource extends RangerGdsBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String                                    name;
        private Long                                      dataShareId;
        private Map<String, RangerPolicyResource>         resource;
        private List<String>                              subResourceNames;
        private String                                    resourceSignature;
        private String                                    conditionExpr;
        private Set<String>                               accessTypes;
        private RangerPolicyItemRowFilterInfo             rowFilter;
        private Map<String, RangerPolicyItemDataMaskInfo> subResourceMasks;
        private Set<String>                               profiles;

        public RangerSharedResource() { }

        public String getName() { return name; }

        public void setName(String name) { this.name = name; }

        public Long getDataShareId() { return dataShareId; }

        public void setDataShareId(Long dataShareId) { this.dataShareId = dataShareId; }

        public Map<String, RangerPolicyResource> getResource() { return resource; }

        public void setResource(Map<String, RangerPolicyResource> resource) { this.resource = resource; }

        public List<String> getSubResourceNames() { return subResourceNames; }

        public void setSubResourceNames(List<String> subResourceNames) { this.subResourceNames = subResourceNames; }

        public String getResourceSignature() { return resourceSignature; }

        public void setResourceSignature(String resourceSignature) { this.resourceSignature = resourceSignature; }

        public String getConditionExpr() { return conditionExpr; }

        public void setConditionExpr(String conditionExpr) { this.conditionExpr = conditionExpr; }

        public Set<String> getAccessTypes() {
            return accessTypes;
        }

        public void setAccessTypes(Set<String> accessTypes) {
            this.accessTypes = accessTypes;
        }

        public RangerPolicyItemRowFilterInfo getRowFilter() { return rowFilter; }

        public void setRowFilter(RangerPolicyItemRowFilterInfo rowFilter) { this.rowFilter = rowFilter; }

        public Map<String, RangerPolicyItemDataMaskInfo> getSubResourceMasks() { return subResourceMasks; }

        public void setSubResourceMasks(Map<String, RangerPolicyItemDataMaskInfo> subResourceMasks) { this.subResourceMasks = subResourceMasks; }

        public Set<String> getProfiles() { return profiles; }

        public void setProfiles(Set<String> profiles) { this.profiles = profiles; }

        public StringBuilder toString(StringBuilder sb) {
            sb.append("RangerSharedResource={");

            super.toString(sb);

            sb.append("name").append(name).append("} ")
              .append("dataShareId={").append(dataShareId).append("} ")
              .append("resource={").append(resource).append("} ")
              .append("subResourceNames={").append(subResourceNames).append("} ")
              .append("resourceSignature={").append(resourceSignature).append("} ")
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
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
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


        public RangerDataShareInDataset() { }

        public Long getDataShareId() { return dataShareId; }

        public void setDataShareId(Long dataShareId) { this.dataShareId = dataShareId; }

        public Long getDatasetId() { return datasetId; }

        public void setDatasetId(Long datasetId) { this.datasetId = datasetId; }

        public GdsShareStatus getStatus() { return status; }

        public void setStatus(GdsShareStatus status) { this.status = status; }

        public RangerValiditySchedule getValiditySchedule() { return validitySchedule; }

        public void setValiditySchedule(RangerValiditySchedule validitySchedule) { this.validitySchedule = validitySchedule; }

        public Set<String> getProfiles() { return profiles; }

        public void setProfiles(Set<String> profiles) { this.profiles = profiles; }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            sb.append("RangerDataShareInDataset={");

            super.toString(sb);

            sb.append("dataShareId={").append(dataShareId).append("} ")
              .append("datasetId={").append(datasetId).append("} ")
              .append("status={").append(status).append("} ")
              .append("validitySchedule={").append(validitySchedule).append("} ")
              .append("profiles={").append(profiles).append("} ")
              .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
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


        public RangerDatasetInProject() { }

        public Long getDatasetId() { return datasetId; }

        public void setDatasetId(Long datasetId) { this.datasetId = datasetId; }

        public Long getProjectId() { return projectId; }

        public void setProjectId(Long projectId) { this.projectId = projectId; }

        public GdsShareStatus getStatus() { return status; }

        public void setStatus(GdsShareStatus status) { this.status = status; }

        public RangerValiditySchedule getValiditySchedule() { return validitySchedule; }

        public void setValiditySchedule(RangerValiditySchedule validitySchedule) { this.validitySchedule = validitySchedule; }

        public Set<String> getProfiles() { return profiles; }

        public void setProfiles(Set<String> profiles) { this.profiles = profiles; }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            sb.append("RangerDatasetInProject={");

            super.toString(sb);

            sb.append("datasetGuid={").append(datasetId).append("} ")
              .append("projectGuid={").append(projectId).append("} ")
              .append("status={").append(status).append("} ")
              .append("validitySchedule={").append(validitySchedule).append("} ")
              .append("profiles={").append(profiles).append("} ")
              .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerGdsObjectACL implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private Map<String, GdsPermission> users;
        private Map<String, GdsPermission> groups;
        private Map<String, GdsPermission> roles;


        public RangerGdsObjectACL() { }

        public Map<String, GdsPermission> getUsers() { return users; }

        public void setUsers(Map<String, GdsPermission> users) { this.users = users; }

        public Map<String, GdsPermission> getGroups() { return groups; }

        public void setGroups(Map<String, GdsPermission> groups) { this.groups = groups; }

        public Map<String, GdsPermission> getRoles() { return roles; }

        public void setRoles(Map<String, GdsPermission> roles) { this.roles = roles; }

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
}
