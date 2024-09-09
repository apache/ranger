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

import org.apache.ranger.plugin.model.RangerSecurityZone.RangerSecurityZoneService;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown=true)
public class RangerSecurityZoneV2 extends RangerBaseModelObject implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

    private String                                   name;
    private String                                   description;
    private Map<String, RangerSecurityZoneServiceV2> services;
    private List<String>                             tagServices;
    private List<RangerPrincipal>                    admins;
    private List<RangerPrincipal>                    auditors;

    public RangerSecurityZoneV2() {
        this(null, null, null, null, null, null);
    }

    public RangerSecurityZoneV2(String name, String description, Map<String, RangerSecurityZoneServiceV2> services, List<String> tagServices, List<RangerPrincipal> admins, List<RangerPrincipal> auditors) {
        setName(name);
        setDescription(description);
        setServices(services);
        setTagServices(tagServices);
        setAdmins(admins);
        setAuditors(auditors);
    }

    public RangerSecurityZoneV2(RangerSecurityZone other) {
        setId(other.getId());
        setGuid(other.getGuid());
        setIsEnabled(other.getIsEnabled());
        setCreatedBy(other.getCreatedBy());
        setUpdatedBy(other.getUpdatedBy());
        setCreateTime(other.getCreateTime());
        setUpdateTime(other.getUpdateTime());
        setVersion(other.getVersion());
        setName(other.getName());
        setDescription(other.getDescription());
        setTagServices((other.getTagServices() != null) ? new ArrayList<>(other.getTagServices()) : new ArrayList<>());
        setAdmins(toPrincipals(other.getAdminUsers(), other.getAdminUserGroups(), other.getAdminRoles()));
        setAuditors(toPrincipals(other.getAuditUsers(), other.getAuditUserGroups(), other.getAuditRoles()));

        services = new HashMap<>();

        if (other.getServices() != null) {
            for (Map.Entry<String, RangerSecurityZoneService> entry : other.getServices().entrySet()) {
                services.put(entry.getKey(), new RangerSecurityZoneServiceV2(entry.getValue()));
            }
        }
    }

    public String getName() { return name; }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() { return description; }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, RangerSecurityZoneServiceV2> getServices() { return services; }

    public void setServices(Map<String, RangerSecurityZoneServiceV2> services) {
        this.services = services == null ? new HashMap<>() : services;
    }

    public List<String> getTagServices() {
        return tagServices;
    }

    public void setTagServices(List<String> tagServices) {
        this.tagServices = (tagServices != null) ? tagServices : new ArrayList<>();
    }

    public List<RangerPrincipal> getAdmins() { return admins; }

    public void setAdmins(List<RangerPrincipal> admins) {
        this.admins = admins == null ? new ArrayList<>() : admins;
    }

    public List<RangerPrincipal> getAuditors() { return auditors; }

    public void setAuditors(List<RangerPrincipal> auditors) {
        this.auditors = auditors == null ? new ArrayList<>() : auditors;
    }

    public RangerSecurityZone toV1() {
        RangerSecurityZone ret = new RangerSecurityZone();

        ret.setId(getId());
        ret.setGuid(getGuid());
        ret.setIsEnabled(getIsEnabled());
        ret.setCreatedBy(getCreatedBy());
        ret.setUpdatedBy(getUpdatedBy());
        ret.setCreateTime(getCreateTime());
        ret.setUpdateTime(getUpdateTime());
        ret.setVersion(getVersion());
        ret.setName(name);
        ret.setDescription(description);

        if (services != null) {
            for (Map.Entry<String, RangerSecurityZoneServiceV2> entry : services.entrySet()) {
                ret.getServices().put(entry.getKey(), entry.getValue().toV1());
            }
        }

        if (tagServices != null) {
            ret.getTagServices().addAll(tagServices);
        }

        fromPrincipals(admins, ret.getAdminUsers(), ret.getAdminUserGroups(), ret.getAdminRoles());
        fromPrincipals(auditors, ret.getAuditUsers(), ret.getAuditUserGroups(), ret.getAuditRoles());

        return ret;
    }

    @Override
    public String toString() {
        return    "{name=" + name
                + ", description="+ description
                + ", services=" + services
                + ", tagServices=" + tagServices
                + ", admins=" + admins
                + ", auditors=" + auditors
                +"}";
    }

    @JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class RangerSecurityZoneServiceV2 implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private List<RangerSecurityZoneResource> resources;

        public RangerSecurityZoneServiceV2() {
            this((List<RangerSecurityZoneResource>) null);
        }

        public RangerSecurityZoneServiceV2(List<RangerSecurityZoneResource> resources) {
            setResources(resources);
        }

        public RangerSecurityZoneServiceV2(RangerSecurityZoneService other) {
            resources = new ArrayList<>();

            if (other != null && other.getResources() != null) {
                for (int i = 0; i < other.getResources().size(); i++) {
                    RangerSecurityZoneResource resource = getResourceAt(other, i);

                    if (resource != null) {
                        resources.add(resource);
                    }
                }
            }
        }

        public List<RangerSecurityZoneResource> getResources() { return resources; }

        public void setResources(List<RangerSecurityZoneResource> resources) {
            this.resources = resources == null ? new ArrayList<>() : resources;
        }

        public RangerSecurityZoneService toV1() {
            RangerSecurityZoneService ret = new RangerSecurityZoneService();

            if (resources != null) {
                for (RangerSecurityZoneResource resource : resources) {
                    ret.getResources().add((HashMap<String, List<String>> ) resource.getResource());
                    ret.getResourcesBaseInfo().add(new RangerSecurityZoneResourceBase(resource));
                }
            }
            return ret;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{resources=[");
            if (resources != null) {
                for (RangerSecurityZoneResource resource : resources) {
                    if (resource != null) {
                        resource.toString(sb).append(" ");
                    }
                }
            }
            sb.append("]}");

            return sb.toString();
        }

        private RangerSecurityZoneResource getResourceAt(RangerSecurityZoneService zoneService, int idx) {
            Map<String, List<String>>      resource = zoneService.getResources() != null && zoneService.getResources().size() > idx ? zoneService.getResources().get(idx) : null;
            RangerSecurityZoneResourceBase baseInfo = zoneService.getResourcesBaseInfo() != null && zoneService.getResourcesBaseInfo().size() > idx ? zoneService.getResourcesBaseInfo().get(idx) : null;

            return resource != null ? new RangerSecurityZoneResource(resource, baseInfo) : null;
        }
    }

    @JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class RangerSecurityZoneResourceBase implements java.io.Serializable {
        private Long    id;
        private String  createdBy;
        private String  updatedBy;
        private Date    createTime;
        private Date    updateTime;

        public RangerSecurityZoneResourceBase() { }

        public RangerSecurityZoneResourceBase(RangerSecurityZoneResourceBase other) {
            if (other != null) {
                setId(other.getId());
                setCreatedBy(other.getCreatedBy());
                setCreateTime(other.getCreateTime());
                setUpdatedBy(other.getUpdatedBy());
                setUpdateTime(other.getUpdateTime());
            }
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getCreatedBy() {
            return createdBy;
        }

        public void setCreatedBy(String createdBy) {
            this.createdBy = createdBy;
        }

        public String getUpdatedBy() {
            return updatedBy;
        }

        public void setUpdatedBy(String updatedBy) {
            this.updatedBy = updatedBy;
        }

        public Date getCreateTime() {
            return createTime;
        }

        public void setCreateTime(Date createTime) {
            this.createTime = createTime;
        }

        public Date getUpdateTime() {
            return updateTime;
        }

        public void setUpdateTime(Date updateTime) {
            this.updateTime = updateTime;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            if (sb == null) {
                sb = new StringBuilder();
            }

            sb.append("{id=").append(id)
              .append(", createdBy=").append(createdBy)
              .append(", createTime=").append(createTime)
              .append(", updatedBy=").append(updatedBy)
              .append(", updateTime=").append(updateTime)
              .append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class RangerSecurityZoneResource extends RangerSecurityZoneResourceBase implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private Map<String, List<String>> resource;

        public RangerSecurityZoneResource() { this(null, null); }

        public RangerSecurityZoneResource(Map<String, List<String>> resource) {
            this(resource, null);
        }

        public RangerSecurityZoneResource(Map<String, List<String>> resource, RangerSecurityZoneResourceBase baseObj) {
            super(baseObj);

            setResource(resource);
        }

        public Map<String, List<String>> getResource() { return resource; }

        public void setResource(Map<String, List<String>> resource) { this.resource = resource == null ? new HashMap<>() : resource; }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        @Override
        public StringBuilder toString(StringBuilder sb) {
            if (sb == null) {
                sb = new StringBuilder();
            }

            sb.append("{resource=");
            super.toString(sb);
            if (resource != null) {
                for (Map.Entry<String, List<String>> entry : resource.entrySet()) {
                    sb.append("{resource-def-name=").append(entry.getKey())
                      .append(", values=").append(entry.getValue()).append("} ");
                }
            }
            sb.append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class RangerSecurityZoneChangeRequest implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String                                   name;
        private String                                   description;
        private Map<String, RangerSecurityZoneServiceV2> resourcesToUpdate;
        private Map<String, RangerSecurityZoneServiceV2> resourcesToRemove;
        private List<String>                             tagServicesToAdd;
        private List<String>                             tagServicesToRemove;
        private List<RangerPrincipal>                    adminsToAdd;
        private List<RangerPrincipal>                    adminsToRemove;
        private List<RangerPrincipal>                    auditorsToAdd;
        private List<RangerPrincipal>                    auditorsToRemove;

        public RangerSecurityZoneChangeRequest() { }

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

        public Map<String, RangerSecurityZoneServiceV2> getResourcesToUpdate() {
            return resourcesToUpdate;
        }

        public void setResourcesToUpdate(Map<String, RangerSecurityZoneServiceV2> resourcesToUpdate) {
            this.resourcesToUpdate = resourcesToUpdate;
        }

        public Map<String, RangerSecurityZoneServiceV2> getResourcesToRemove() {
            return resourcesToRemove;
        }

        public void setResourcesToRemove(Map<String, RangerSecurityZoneServiceV2> resourcesToRemove) {
            this.resourcesToRemove = resourcesToRemove;
        }

        public List<String> getTagServicesToAdd() {
            return tagServicesToAdd;
        }

        public void setTagServicesToAdd(List<String> tagServicesToAdd) {
            this.tagServicesToAdd = tagServicesToAdd;
        }

        public List<String> getTagServicesToRemove() {
            return tagServicesToRemove;
        }

        public void setTagServicesToRemove(List<String> tagServicesToRemove) {
            this.tagServicesToRemove = tagServicesToRemove;
        }

        public List<RangerPrincipal> getAdminsToAdd() {
            return adminsToAdd;
        }

        public void setAdminsToAdd(List<RangerPrincipal> adminsToAdd) {
            this.adminsToAdd = adminsToAdd;
        }

        public List<RangerPrincipal> getAdminsToRemove() {
            return adminsToRemove;
        }

        public void setAdminsToRemove(List<RangerPrincipal> adminsToRemove) {
            this.adminsToRemove = adminsToRemove;
        }

        public List<RangerPrincipal> getAuditorsToAdd() {
            return auditorsToAdd;
        }

        public void setAuditorsToAdd(List<RangerPrincipal> auditorsToAdd) {
            this.auditorsToAdd = auditorsToAdd;
        }

        public List<RangerPrincipal> getAuditorsToRemove() {
            return auditorsToRemove;
        }

        public void setAuditorsToRemove(List<RangerPrincipal> auditorsToRemove) {
            this.auditorsToRemove = auditorsToRemove;
        }


    }

    private void fromPrincipals(List<RangerPrincipal> principals, List<String> users, List<String> groups, List<String> roles) {
        if (principals != null) {
            for (RangerPrincipal principal : principals) {
                if (principal.getType() == RangerPrincipal.PrincipalType.USER) {
                    users.add(principal.getName());
                } else if (principal.getType() == RangerPrincipal.PrincipalType.GROUP) {
                    groups.add(principal.getName());
                } else if (principal.getType() == RangerPrincipal.PrincipalType.ROLE) {
                    roles.add(principal.getName());
                }
            }
        }
    }

    private List<RangerPrincipal> toPrincipals(List<String> users, List<String> groups, List<String> roles) {
        List<RangerPrincipal> ret = new ArrayList<>();

        if (users != null) {
            for (String name : users) {
                ret.add(new RangerPrincipal(RangerPrincipal.PrincipalType.USER, name));
            }
        }

        if (groups != null) {
            for (String name : groups) {
                ret.add(new RangerPrincipal(RangerPrincipal.PrincipalType.GROUP, name));
            }
        }

        if (roles != null) {
            for (String name : roles) {
                ret.add(new RangerPrincipal(RangerPrincipal.PrincipalType.ROLE, name));
            }
        }

        return ret;
    }
}

