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

package org.apache.ranger.validation;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.RoleDBStore;
import org.apache.ranger.biz.ServiceMgr;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.RangerRoleCache;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXGdsDataShare;
import org.apache.ranger.entity.XXGdsDataset;
import org.apache.ranger.entity.XXGdsProject;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXRole;
import org.apache.ranger.entity.XXSecurityZone;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShare;
import org.apache.ranger.plugin.model.RangerGds.RangerDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerProject;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerRolesUtil;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.service.RangerGdsDataShareService;
import org.apache.ranger.service.RangerGdsDatasetService;
import org.apache.ranger.service.RangerGdsProjectService;
import org.apache.ranger.service.RangerServiceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.ranger.db.XXGlobalStateDao.RANGER_GLOBAL_STATE_NAME_ROLE;

@Component
public class RangerGdsValidationDBProvider extends RangerGdsValidationDataProvider {
    private static final Logger LOG = LoggerFactory.getLogger(RangerGdsValidationDBProvider.class);

    private static final String SERVICE_NAME_FOR_ROLES = "";

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    XUserMgr userMgr;

    @Autowired
    ServiceMgr serviceMgr;

    @Autowired
    RangerServiceService svcService;

    @Autowired
    RangerGdsDatasetService datasetService;

    @Autowired
    RangerGdsProjectService projectService;

    @Autowired
    RangerGdsDataShareService dataShareService;

    @Autowired
    RoleDBStore rolesStore;

    @Autowired
    RangerBizUtil bizUtil;

    RangerRolesUtil rolesUtil;

    public RangerGdsValidationDBProvider() {
    }

    public Long getServiceId(String name) {
        XXService obj = daoMgr.getXXService().findByName(name);

        return obj != null ? obj.getId() : null;
    }

    public Long getZoneId(String name) {
        XXSecurityZone obj = daoMgr.getXXSecurityZoneDao().findByZoneName(name);

        return obj != null ? obj.getId() : null;
    }

    public Long getDatasetId(String name) {
        XXGdsDataset obj = daoMgr.getXXGdsDataset().findByName(name);

        return obj != null ? obj.getId() : null;
    }

    public Long getProjectId(String name) {
        XXGdsProject obj = daoMgr.getXXGdsProject().findByName(name);

        return obj != null ? obj.getId() : null;
    }

    public Long getDataShareId(String name) {
        XXGdsDataShare obj = daoMgr.getXXGdsDataShare().findByName(name);

        return obj != null ? obj.getId() : null;
    }

    public Long getUserId(String name) {
        XXUser obj = daoMgr.getXXUser().findByUserName(name);

        return obj != null ? obj.getId() : null;
    }

    public Long getGroupId(String name) {
        XXGroup obj = daoMgr.getXXGroup().findByGroupName(name);

        return obj != null ? obj.getId() : null;
    }

    public Long getRoleId(String name) {
        XXRole obj = daoMgr.getXXRole().findByRoleName(name);

        return obj != null ? obj.getId() : null;
    }

    public String getCurrentUserLoginId() {
        return bizUtil.getCurrentUserLoginId();
    }

    public boolean isAdminUser() {
        return bizUtil.isAdmin();
    }

    public boolean isServiceAdmin(String name) {
        XXService     xService = daoMgr.getXXService().findByName(name);
        RangerService service  = xService != null ? svcService.getPopulatedViewObject(xService) : null;

        return service != null && bizUtil.isUserServiceAdmin(service, bizUtil.getCurrentUserLoginId());
    }

    public boolean isZoneAdmin(String zoneName) {
        return serviceMgr.isZoneAdmin(zoneName);
    }

    public Set<String> getGroupsForUser(String userName) {
        return userMgr.getGroupsForUser(userName);
    }

    public Set<String> getRolesForUser(String userName) {
        RangerRolesUtil rolesUtil = initGetRolesUtil();

        return rolesUtil != null && rolesUtil.getUserRoleMapping() != null ? rolesUtil.getUserRoleMapping().get(userName) : null;
    }

    public Set<String> getRolesForUserAndGroups(String userName, Collection<String> groups) {
        RangerRolesUtil rolesUtil = initGetRolesUtil();
        Set<String>     ret       = getRolesForUser(userName);

        if (rolesUtil != null) {
            final Map<String, Set<String>> groupRoleMapping = rolesUtil.getGroupRoleMapping();

            if (MapUtils.isNotEmpty(groupRoleMapping)) {
                if (CollectionUtils.isNotEmpty(groups)) {
                    for (String group : groups) {
                        ret = addRoles(ret, groupRoleMapping.get(group));
                    }
                }

                ret = addRoles(ret, groupRoleMapping.get(RangerPolicyEngine.GROUP_PUBLIC));
            }
        }

        return ret;
    }

    public Set<String> getAccessTypes(String serviceName) {
        List<String> accessTypes = daoMgr.getXXAccessTypeDef().getNamesByServiceName(serviceName);
        Set<String>  ret         = new HashSet<>(accessTypes);

        ret.addAll(ServiceDefUtil.ACCESS_TYPE_MARKERS);

        return ret;
    }

    public Set<String> getMaskTypes(String serviceName) {
        List<String> maskTypes = daoMgr.getXXDataMaskTypeDef().getNamesByServiceName(serviceName);

        return new HashSet<>(maskTypes);
    }

    public RangerDataset getDataset(Long id) {
        RangerDataset ret = null;

        if (id != null) {
            try {
                ret = datasetService.read(id);
            } catch (Exception excp) {
                LOG.debug("failed to get dataset with id={}", id, excp);

                // ignore
            }
        }

        return ret;
    }

    public RangerProject getProject(Long id) {
        RangerProject ret = null;

        if (id != null) {
            try {
                ret = projectService.read(id);
            } catch (Exception excp) {
                LOG.debug("failed to get project with id={}", id, excp);

                // ignore
            }
        }

        return ret;
    }

    public RangerDataShare getDataShare(Long id) {
        RangerDataShare ret = null;

        try {
            ret = dataShareService.read(id);
        } catch (Exception excp) {
            LOG.debug("failed to get DataShare with id={}", id, excp);

            // ignore
        }

        return ret;
    }

    public Long getSharedResourceId(Long dataShareId, String name) {
        return daoMgr.getXXGdsSharedResource().getIdByDataShareIdAndName(dataShareId, name);
    }

    public Long getSharedResourceId(Long dataShareId, RangerPolicyResourceSignature signature) {
        return daoMgr.getXXGdsSharedResource().getIdByDataShareIdAndResourceSignature(dataShareId, signature.getSignature());
    }

    private RangerRolesUtil initGetRolesUtil() {
        RangerRolesUtil ret              = this.rolesUtil;
        Long            lastKnownVersion = ret != null ? ret.getRoleVersion() : null;
        Long            currentVersion   = daoMgr.getXXGlobalState().getAppDataVersion(RANGER_GLOBAL_STATE_NAME_ROLE);

        if (lastKnownVersion == null || !lastKnownVersion.equals(currentVersion)) {
            synchronized (this) {
                ret              = this.rolesUtil;
                lastKnownVersion = ret != null ? ret.getRoleVersion() : null;

                if (lastKnownVersion == null || !lastKnownVersion.equals(currentVersion)) {
                    try {
                        RangerRoles roles = RangerRoleCache.getInstance().getLatestRangerRoleOrCached(SERVICE_NAME_FOR_ROLES, rolesStore, lastKnownVersion, currentVersion);

                        if (roles != null) {
                            ret = new RangerRolesUtil(roles);
                            this.rolesUtil = new RangerRolesUtil(roles);
                        }
                    } catch (Exception excp) {
                        LOG.warn("failed to get roles from store", excp);
                    }
                } else {
                    LOG.debug("roles already initialized to latest version {}", currentVersion);
                }
            }
        }

        return ret;
    }

    private Set<String> addRoles(Set<String> allRoles, Set<String> rolesToAdd) {
        if (CollectionUtils.isNotEmpty(rolesToAdd)) {
            if (allRoles == null) {
                allRoles = new HashSet<>();
            }

            allRoles.addAll(rolesToAdd);
        }

        return allRoles;
    }
}
