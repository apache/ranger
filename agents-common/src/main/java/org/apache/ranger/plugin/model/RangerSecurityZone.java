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

import org.apache.ranger.plugin.model.RangerPrincipal.PrincipalType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import org.apache.ranger.plugin.model.RangerSecurityZoneV2.RangerSecurityZoneResourceBase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown=true)
public class RangerSecurityZone extends RangerBaseModelObject implements java.io.Serializable {
    public static final long RANGER_UNZONED_SECURITY_ZONE_ID = 1L;
	private static final long serialVersionUID = 1L;
    private String                                  name;
    private Map<String, RangerSecurityZoneService>  services;
    private List<String>  							tagServices;
    private List<String>                            adminUsers;
    private List<String>                            adminUserGroups;
    private List<String>                            adminRoles;
    private List<String>                            auditUsers;
    private List<String>                            auditUserGroups;
    private List<String>                            auditRoles;
    private String                                  description;

    public RangerSecurityZone() {
        this(null, null, null, null, null, null, null,null, null, null);
    }

    public RangerSecurityZone(String name, Map<String, RangerSecurityZoneService> services,List<String> tagServices, List<String> adminUsers, List<String> adminUserGroups, List<String> auditUsers, List<String> auditUserGroups, String description) {
        this(name, services, tagServices, adminUsers, adminUserGroups, null, auditUsers, auditUserGroups, null, description);
    }

    public RangerSecurityZone(String name, Map<String, RangerSecurityZoneService> services,List<String> tagServices, List<String> adminUsers, List<String> adminUserGroups, List<String> adminRoles, List<String> auditUsers, List<String> auditUserGroups, List<String> auditRoles, String description) {
        setName(name);
        setServices(services);
        setAdminUsers(adminUsers);
        setAdminUserGroups(adminUserGroups);
        setAdminRoles(adminRoles);
        setAuditUsers(auditUsers);
        setAuditUserGroups(auditUserGroups);
        setAuditRoles(auditRoles);
        setDescription(description);
        setTagServices(tagServices);
    }
    public String getName() { return name; }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() { return description; }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, RangerSecurityZoneService> getServices() { return services; }

    public void setServices(Map<String, RangerSecurityZoneService> services) {
        this.services = services == null ? new HashMap<>() : services;
    }

    public List<String> getAdminUsers() { return adminUsers; }

    public void setAdminUsers(List<String> adminUsers) {
        this.adminUsers = adminUsers == null ? new ArrayList<>() : adminUsers;
    }

    public List<String> getAdminUserGroups() { return adminUserGroups; }

    public void setAdminUserGroups(List<String> adminUserGroups) {
        this.adminUserGroups = adminUserGroups == null ? new ArrayList<>() : adminUserGroups;
    }

    public List<String> getAdminRoles() { return adminRoles; }

    public void setAdminRoles(List<String> adminRoles) {
        this.adminRoles = adminRoles == null ? new ArrayList<>() : adminRoles;
    }

    public List<String> getAuditUsers() { return auditUsers; }

    public void setAuditUsers(List<String> auditUsers) {
        this.auditUsers = auditUsers == null ? new ArrayList<>() : auditUsers;
    }

    public List<String> getAuditUserGroups() { return auditUserGroups; }

    public void setAuditUserGroups(List<String> auditUserGroups) {
        this.auditUserGroups = auditUserGroups == null ? new ArrayList<>() : auditUserGroups;
    }

    public List<String> getAuditRoles() { return auditRoles; }

    public void setAuditRoles(List<String> auditRoles) {
        this.auditRoles = auditRoles == null ? new ArrayList<>() : auditRoles;
    }

    public List<String> getTagServices() {
                return tagServices;
        }

        public void setTagServices(List<String> tagServices) {
                this.tagServices = (tagServices != null) ? tagServices : new ArrayList<String>(); 
        }

        @Override
    public String toString() {
        return    "{name=" + name
                + ", services=" + services
                + ", tagServices=" + tagServices
                + ", adminUsers=" + adminUsers
                + ", adminUserGroups=" + adminUserGroups
                + ", adminRoles=" + adminRoles
                + ", auditUsers=" + auditUsers
                + ", auditUserGroups=" + auditUserGroups
                + ", auditRoles=" + auditRoles
                + ", description="+ description
                +"}";
    }

	@JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
	@JsonIgnoreProperties(ignoreUnknown=true)
	public static class RangerSecurityZoneService implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
        private List<HashMap<String, List<String>>>  resources;
        private List<RangerSecurityZoneResourceBase> resourcesBaseInfo;

        public RangerSecurityZoneService() {
            this(null, null);
        }

        public RangerSecurityZoneService(List<HashMap<String, List<String>>> resources) {
            this(resources, null);
        }

        public RangerSecurityZoneService(List<HashMap<String, List<String>>> resources, List<RangerSecurityZoneResourceBase> resourcesBaseInfo) {
            setResources(resources);
            setResourcesBaseInfo(resourcesBaseInfo);
        }

        public List<HashMap<String, List<String>>> getResources() { return resources; }

        public void setResources(List<HashMap<String, List<String>>> resources) {
            this.resources = resources == null ? new ArrayList<>() : resources;
        }

        public List<RangerSecurityZoneResourceBase> getResourcesBaseInfo() { return resourcesBaseInfo; }

        public void setResourcesBaseInfo(List<RangerSecurityZoneResourceBase> resourcesBaseInfo) {
            this.resourcesBaseInfo = resourcesBaseInfo == null ? new ArrayList<>() : resourcesBaseInfo;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{resources=[");
            if (resources != null) {
                for (int i = 0; i < resources.size(); i++) {
                    HashMap<String, List<String>>  resource = resources.get(i);
                    RangerSecurityZoneResourceBase baseInfo = (resourcesBaseInfo != null && resourcesBaseInfo.size() > i) ? resourcesBaseInfo.get(i) : null;

                    sb.append("{resource=");
                    if (resource != null) {
                        for (Map.Entry<String, List<String>> entry : resource.entrySet()) {
                            sb.append("{resource-def-name=").append(entry.getKey()).append(", values=").append(entry.getValue()).append("} ");
                        }
                    }
                    sb.append("} ");

                    sb.append("{baseInfo=");
                    if (baseInfo != null) {
                        baseInfo.toString(sb);
                    }
                    sb.append("} ");
                }
            }
            sb.append("]}");

            return sb.toString();
        }
    }

    @JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class SecurityZoneSummary extends RangerBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String                      name;
        private String                      description;
        private Long                        totalResourceCount;
        private Map<PrincipalType, Integer> adminCount;
        private Map<PrincipalType, Integer> auditorCount;
        private List<String>                tagServices;
        private List<ZoneServiceSummary>    services;

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

        public Long getTotalResourceCount() {
            return totalResourceCount;
        }

        public void setTotalResourceCount(Long totalResourceCount) {
            this.totalResourceCount = totalResourceCount;
        }

        public Map<PrincipalType, Integer> getAdminCount() {
            return adminCount;
        }

        public void setAdminCount(Map<PrincipalType, Integer> adminCount) {
            this.adminCount = adminCount;
        }

        public Map<PrincipalType, Integer> getAuditorCount() {
            return auditorCount;
        }

        public void setAuditorCount(Map<PrincipalType, Integer> auditorCount) {
            this.auditorCount = auditorCount;
        }

        public List<String> getTagServices() {
            return tagServices;
        }

        public void setTagServices(List<String> tagServices) {
            this.tagServices = tagServices;
        }

        public List<ZoneServiceSummary> getServices() {
            return services;
        }

        public void setServices(List<ZoneServiceSummary> services) {
            this.services = services;
        }
    }

    public static class ZoneServiceSummary implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private Long   id;
        private String name;
        private String type;
        private String displayName;
        private Long   resourceCount;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Long getResourceCount() {
            return resourceCount;
        }

        public void setResourceCount(Long resourceCount) {
            this.resourceCount = resourceCount;
        }

        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }
    }
}

