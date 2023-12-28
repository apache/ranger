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

package org.apache.ranger.plugin.util;

import org.apache.ranger.plugin.model.RangerPrincipal;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.apache.ranger.plugin.model.RangerSecurityZone.RangerSecurityZoneService;
import org.apache.ranger.plugin.model.RangerSecurityZoneV2.RangerSecurityZoneChangeRequest;
import org.apache.ranger.plugin.model.RangerSecurityZoneV2.RangerSecurityZoneResource;
import org.apache.ranger.plugin.model.RangerSecurityZoneV2.RangerSecurityZoneResourceBase;
import org.apache.ranger.plugin.model.RangerSecurityZoneV2.RangerSecurityZoneServiceV2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RangerSecurityZoneHelper {
    private final RangerSecurityZone                           zone;
    private final String                                       currentUser;
    private final Map<String, RangerSecurityZoneServiceHelper> services;


    public RangerSecurityZoneHelper(RangerSecurityZone zone, String currentUser) {
        this.zone        = zone;
        this.currentUser = currentUser;
        this.services    = new HashMap<>();

        for (Map.Entry<String, RangerSecurityZoneService> entry : zone.getServices().entrySet()) {
            this.services.put(entry.getKey(), new RangerSecurityZoneServiceHelper(entry.getValue(), currentUser));
        }
    }

    public RangerSecurityZone getZone() { return zone; }

    public RangerSecurityZoneServiceHelper getZoneService(String serviceName) {
        return services.get(serviceName);
    }

    public RangerSecurityZoneServiceHelper addZoneService(String serviceName) {
        RangerSecurityZoneServiceHelper ret = services.get(serviceName);

        if (ret == null) {
            RangerSecurityZoneService zoneService = zone.getServices().get(serviceName);

            if (zoneService == null) {
                zoneService = new RangerSecurityZoneService();

                zone.getServices().put(serviceName, zoneService);
            }

            ret = new RangerSecurityZoneServiceHelper(zoneService, currentUser);

            services.put(serviceName, ret);
        }

        return ret;
    }

    public void removeService(String serviceName) {
        services.remove(serviceName);
        zone.getServices().remove(serviceName);
    }

    public RangerSecurityZone updateZone(RangerSecurityZoneChangeRequest changeData) throws Exception {
        if (changeData.getName() != null) {
            zone.setName(changeData.getName());
        }

        if (changeData.getDescription() != null) {
            zone.setDescription(changeData.getDescription());
        }

        if (changeData.getResourcesToUpdate() != null) {
            for (Map.Entry<String, RangerSecurityZoneServiceV2> entry : changeData.getResourcesToUpdate().entrySet()) {
                String                          serviceName       = entry.getKey();
                RangerSecurityZoneServiceV2     zoneService       = entry.getValue();
                RangerSecurityZoneServiceHelper zoneServiceHelper = addZoneService(serviceName);

                if (zoneService != null && zoneService.getResources() != null) {
                    for (RangerSecurityZoneResource resource : zoneService.getResources()) {
                        if (resource != null) {
                            zoneServiceHelper.updateResource(resource);
                        }
                    }
                }
            }
        }

        if (changeData.getResourcesToRemove() != null) {
            for (Map.Entry<String, RangerSecurityZoneServiceV2> entry : changeData.getResourcesToRemove().entrySet()) {
                String                          serviceName       = entry.getKey();
                RangerSecurityZoneServiceV2     zoneService       = entry.getValue();
                RangerSecurityZoneServiceHelper zoneServiceHelper = getZoneService(serviceName);

                if (zoneServiceHelper != null && zoneService != null && zoneService.getResources() != null) {
                    for (RangerSecurityZoneResource resource : zoneService.getResources()) {
                        if (resource != null) {
                            final RangerSecurityZoneResource removedResource;

                            if (resource.getId() != null) {
                                removedResource = zoneServiceHelper.removeResource(resource.getId());
                            } else if (resource.getResource() != null) {
                                removedResource = zoneServiceHelper.removeResource(resource.getResource());
                            } else {
                                removedResource = null;
                            }

                            if (removedResource == null) {
                                throw new Exception(resource + ": resource not in zone");
                            }
                        }
                    }

                    if (zoneServiceHelper.getResourceCount() == 0) {
                        removeService(serviceName);
                    }
                } else {
                    throw new Exception(serviceName + ": service not in zone");
                }
            }
        }

        if (changeData.getTagServicesToAdd() != null) {
			for (String tagServiceToAdd : changeData.getTagServicesToAdd()) {
				if (!addIfAbsent(tagServiceToAdd, zone.getTagServices())) {
					throw new Exception(tagServiceToAdd + ": tag service already exists in zone");
				}
			}
        }

        if (changeData.getTagServicesToRemove() != null) {
            for (String tagServiceToRemove : changeData.getTagServicesToRemove()) {
                if (!zone.getTagServices().remove(tagServiceToRemove)) {
                    throw new Exception(tagServiceToRemove + ": tag service not in zone");
                }
            }
        }

        if (changeData.getAdminsToAdd() != null) {
            addPrincipals(changeData.getAdminsToAdd(), zone.getAdminUsers(), zone.getAdminUserGroups(), zone.getAdminRoles());
        }

        if (changeData.getAdminsToRemove() != null) {
            removePrincipals(changeData.getAdminsToRemove(), zone.getAdminUsers(), zone.getAdminUserGroups(), zone.getAdminRoles());
        }

        if (changeData.getAuditorsToAdd() != null) {
            addPrincipals(changeData.getAuditorsToAdd(), zone.getAuditUsers(), zone.getAuditUserGroups(), zone.getAuditRoles());
        }

        if (changeData.getAuditorsToRemove() != null) {
            removePrincipals(changeData.getAuditorsToRemove(), zone.getAuditUsers(), zone.getAuditUserGroups(), zone.getAuditRoles());
        }

        return zone;
    }

    private void addPrincipals(List<RangerPrincipal> principals, List<String> users, List<String> groups, List<String> roles) throws Exception {
        for (RangerPrincipal principal : principals) {
            boolean isAdded = false;

            if (principal.getType() == RangerPrincipal.PrincipalType.USER) {
                isAdded = addIfAbsent(principal.getName(), users);
            } else if (principal.getType() == RangerPrincipal.PrincipalType.GROUP) {
				isAdded = addIfAbsent(principal.getName(), groups);
            } else if (principal.getType() == RangerPrincipal.PrincipalType.ROLE) {
				isAdded = addIfAbsent(principal.getName(), roles);
            }

            if(!isAdded) {
                throw new Exception(principal + ": principal already an admin or auditor in zone");
            }
        }
    }

    private void removePrincipals(List<RangerPrincipal> principals, List<String> users, List<String> groups, List<String> roles) throws Exception {
        for (RangerPrincipal principal : principals) {
            boolean isRemoved = false;

            if (principal.getType() == RangerPrincipal.PrincipalType.USER) {
                isRemoved = users.remove(principal.getName());
            } else if (principal.getType() == RangerPrincipal.PrincipalType.GROUP) {
                isRemoved = groups.remove(principal.getName());
            } else if (principal.getType() == RangerPrincipal.PrincipalType.ROLE) {
                isRemoved = roles.remove(principal.getName());
            }

            if(!isRemoved) {
                throw new Exception(principal + ": principal not an admin or auditor in zone");
            }
        }
    }

    private boolean addIfAbsent(String item, List<String> lst) {
        final boolean ret;

        if (!lst.contains(item)) {
            ret = lst.add(item);
        } else {
            ret = false;
        }

        return ret;
    }

    public static class RangerSecurityZoneServiceHelper {
        private final RangerSecurityZoneService            zoneService;
        private final String                               currentUser;
        private final List<HashMap<String, List<String>>>  resources;
        private final List<RangerSecurityZoneResourceBase> resourcesBaseInfo;
        private       long                                 nextResourceId = 1;

        public RangerSecurityZoneServiceHelper(RangerSecurityZoneService zoneService, String currentUser) {
            this.zoneService = zoneService;
            this.currentUser = currentUser;

            if (zoneService.getResources() != null) {
                this.resources = zoneService.getResources();
            } else {
                this.resources = new ArrayList<>();

                zoneService.setResources(this.resources);
            }

            if (zoneService.getResourcesBaseInfo() != null) {
                this.resourcesBaseInfo = zoneService.getResourcesBaseInfo();
            } else {
                this.resourcesBaseInfo = new ArrayList<>();

                zoneService.setResourcesBaseInfo(this.resourcesBaseInfo);
            }

            // compute nextResourceId
            for (RangerSecurityZoneResourceBase baseInfo : resourcesBaseInfo) {
                if (baseInfo.getId() != null && nextResourceId <= baseInfo.getId()) {
                    nextResourceId = baseInfo.getId() + 1;
                }
            }

            // make sure resourcesBaseInfo has as many entries as resources
            for (int i = resourcesBaseInfo.size(); i < resources.size(); i++) {
                RangerSecurityZoneResourceBase baseInfo = new RangerSecurityZoneResourceBase();

                setCreated(baseInfo);

                resourcesBaseInfo.add(baseInfo);
            }

            // remove any additional resourcesBaseInfo entries
            for (int i = resources.size(); i < resourcesBaseInfo.size(); ) {
                resourcesBaseInfo.remove(i);
            }

            // set missing IDs
            for (RangerSecurityZoneResourceBase baseInfo : resourcesBaseInfo) {
                if (baseInfo.getId() == null) {
                    baseInfo.setId(nextResourceId++);
                }
            }
        }

        public RangerSecurityZoneService getZoneService() { return zoneService; }

        public int getResourceCount() {
            return resources != null ? resources.size() : 0;
        }

        public List<RangerSecurityZoneResource> getResources() {
            List<RangerSecurityZoneResource> ret = new ArrayList<>();

            if (resources != null) {
                for (int i = 0; i < resources.size(); i++) {
                    ret.add(getResourceAt(i));
                }
            }

            return Collections.unmodifiableList(ret);
        }

        public List<RangerSecurityZoneResource> getResources(int startIdx, int count) {
            List<RangerSecurityZoneResource> ret = new ArrayList<>();

            if (resources != null) {
                for (int i = 0; i < count; i++) {
                    RangerSecurityZoneResource resource = getResourceAt(startIdx + i);

                    if (resource == null) {
                        break;
                    }

                    ret.add(resource);
                }
            }

            return Collections.unmodifiableList(ret);
        }

        public RangerSecurityZoneResource getResource(long id) {
            int idx = getResourceIdx(id);

            return idx != -1 ? getResourceAt(idx) : null;
        }

        public RangerSecurityZoneResource getResource(Map<String, List<String>> resource) {
            int idx = getResourceIdx(resource);

            return idx != -1 ? getResourceAt(idx) : null;
        }

        public RangerSecurityZoneResource addResource(RangerSecurityZoneResource resource) {
            setCreated(resource);

            resources.add((HashMap<String, List<String>>) resource.getResource());
            resourcesBaseInfo.add(new RangerSecurityZoneResourceBase(resource));

            return resource;
        }

        public RangerSecurityZoneResource updateResource(RangerSecurityZoneResource resource) {
            Long resourceId  = resource.getId();
            int  resourceIdx = resourceId != null ? getResourceIdx(resourceId) : -1;

            if (resourceIdx == -1) {
                addResource(resource);
            } else {
                setUpdated(resource, resourceIdx);

                resources.set(resourceIdx, (HashMap<String, List<String>>) resource.getResource());
                resourcesBaseInfo.set(resourceIdx, new RangerSecurityZoneResourceBase(resource));
            }

            return resource;
       }

        public RangerSecurityZoneResource removeResource(long id) {
            int idx = getResourceIdx(id);

            return idx != -1 ? removeResourceAt(idx) : null;
        }

        public RangerSecurityZoneResource removeResource(Map<String, List<String>> resource) {
            int idx = getResourceIdx(resource);

            return idx != -1 ? removeResourceAt(idx) : null;
        }

        private RangerSecurityZoneResource getResourceAt(int idx) {
            RangerSecurityZoneResource     ret              = null;
            HashMap<String, List<String>>  resource         = (resources != null && resources.size() > idx) ? resources.get(idx) : null;
            RangerSecurityZoneResourceBase resourceBaseInfo = (resourcesBaseInfo != null && resourcesBaseInfo.size() > idx) ? resourcesBaseInfo.get(idx) : null;

            if (resource != null) {
                ret = new RangerSecurityZoneResource(resource, resourceBaseInfo);
            }

            return ret;
        }

        private RangerSecurityZoneResource removeResourceAt(int idx) {
            RangerSecurityZoneResource     ret              = null;
            HashMap<String, List<String>>  resource         = (resources != null && resources.size() > idx) ? resources.remove(idx) : null;
            RangerSecurityZoneResourceBase resourceBaseInfo = (resourcesBaseInfo != null && resourcesBaseInfo.size() > idx) ? resourcesBaseInfo.remove(idx) : null;

            if (resource != null) {
                ret = new RangerSecurityZoneResource(resource, resourceBaseInfo);
            }

            return ret;
        }

        private int getResourceIdx(long id) {
            int ret = -1;

            if (resourcesBaseInfo != null) {
                for (int i = 0; i < resourcesBaseInfo.size(); i++) {
                    RangerSecurityZoneResourceBase baseInfo = resourcesBaseInfo.get(i);

                    if (baseInfo != null && baseInfo.getId() != null && baseInfo.getId().equals(id)) {
                        ret = i;

                        break;
                    }
                }
            }

            return ret;
        }

        private int getResourceIdx(Map<String, List<String>> resource) {
            int ret = -1;

            if (resources != null) {
                for (int i = 0; i < resources.size(); i++) {
                    HashMap<String, List<String>> res = resources.get(i);

                    if (Objects.equals(resource, res)) {
                        ret = i;

                        break;
                    }
                }
            }

            return ret;
        }

        private void setCreated(RangerSecurityZoneResourceBase baseInfo) {
            baseInfo.setId(nextResourceId++);
            baseInfo.setCreatedBy(currentUser);
            baseInfo.setCreateTime(new Date());
            baseInfo.setUpdatedBy(currentUser);
            baseInfo.setUpdateTime(new Date());
        }

        private void setUpdated(RangerSecurityZoneResourceBase baseInfo, int idx) {
            RangerSecurityZoneResourceBase resourceBase = (resourcesBaseInfo != null && resourcesBaseInfo.size() > idx) ? resourcesBaseInfo.get(idx) : null;

            if(resourceBase != null) {
                baseInfo.setId(resourceBase.getId());
                baseInfo.setCreatedBy(resourceBase.getCreatedBy());
                baseInfo.setCreateTime(resourceBase.getCreateTime());
            }

            baseInfo.setUpdatedBy(currentUser);
            baseInfo.setUpdateTime(new Date());
        }
    }
}
