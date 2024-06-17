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

package org.apache.ranger.service;


import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.entity.XXRole;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class RangerRoleService extends RangerRoleServiceBase<XXRole, RangerRole> {

    private static final Logger logger = LoggerFactory.getLogger(RangerRoleService.class);

    public RangerRoleService() {
        super();
    }

    @Override
    protected void validateForCreate(RangerRole vObj) {
    }

    @Override
    protected void validateForUpdate(RangerRole vObj, XXRole entityObj) {
    }

    @Override
    protected XXRole mapViewToEntityBean(RangerRole rangerRole, XXRole xxRole, int OPERATION_CONTEXT) {
        XXRole ret = super.mapViewToEntityBean(rangerRole, xxRole, OPERATION_CONTEXT);
        ret.setRoleText(JsonUtils.objectToJson(rangerRole));
        return ret;
    }
    @Override
    protected RangerRole mapEntityToViewBean(RangerRole rangerRole, XXRole xxRole) {
        RangerRole ret = super.mapEntityToViewBean(rangerRole, xxRole);

        if (StringUtils.isNotEmpty(xxRole.getRoleText())) {
            if (logger.isDebugEnabled()) {
                logger.debug("roleText=" + xxRole.getRoleText());
            }
            RangerRole roleFromJsonData = JsonUtils.jsonToObject(xxRole.getRoleText(), RangerRole.class);

            if (roleFromJsonData != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Role object built from JSON :[" + roleFromJsonData +"]");
                }
                ret.setOptions(roleFromJsonData.getOptions());
                ret.setUsers(roleFromJsonData.getUsers());
                ret.setGroups(roleFromJsonData.getGroups());
                ret.setRoles(roleFromJsonData.getRoles());
                ret.setCreatedByUser(roleFromJsonData.getCreatedByUser());
            }
        } else {
            logger.info("Empty string representing jsonData in [" + xxRole + "]!!");
        }

        return ret;
    }

    public void updatePolicyVersions(Long roleId) {
        if (logger.isDebugEnabled()) {
            logger.debug("==> updatePolicyVersions(roleId=" + roleId + ")");
        }
        // Get all roles which include this role because change to this affects all these roles
        Set<Long> containingRoles = getContainingRoles(roleId);

        if (logger.isDebugEnabled()) {
            logger.debug("All containing Roles for roleId:[" + roleId +"] are [" + containingRoles + "]");
        }

        updatePolicyVersions(containingRoles);

        if (logger.isDebugEnabled()) {
            logger.debug("<== updatePolicyVersions(roleId=" + roleId + ")");
        }
    }

    private Set<Long> getContainingRoles(Long roleId) {
        Set<Long> ret = new HashSet<>();

        addContainingRoles(roleId, ret);

        return ret;
    }

    public void updateRoleVersions(Long roleId) {
        if (logger.isDebugEnabled()) {
            logger.debug("==> updateRoleVersions(roleId=" + roleId + ")");
        }
        // Get all roles which include this role because change to this affects all these roles
        Set<Long> containingRoles = getContainingRoles(roleId);

        if (logger.isDebugEnabled()) {
            logger.debug("All containing Roles for roleId:[" + roleId +"] are [" + containingRoles + "]");
        }

        updateRoleVersions(containingRoles);

        if (logger.isDebugEnabled()) {
            logger.debug("<== updateRoleVersions(roleId=" + roleId + ")");
        }
    }

    private void addContainingRoles(Long roleId, Set<Long> allRoles) {
        if (logger.isDebugEnabled()) {
            logger.debug("==> addContainingRoles(roleId=" + roleId + ")");
        }
        if (!allRoles.contains(roleId)) {
            allRoles.add(roleId);

            Set<Long> roles = daoMgr.getXXRoleRefRole().getContainingRoles(roleId);

            for (Long role : roles) {
                addContainingRoles(role, allRoles);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("<== addContainingRoles(roleId=" + roleId + ")");
        }
    }

    private void updatePolicyVersions(Set<Long> roleIds) {
        if (logger.isDebugEnabled()) {
            logger.debug("==> updatePolicyVersions(roleIds=" + roleIds + ")");
        }

        if (CollectionUtils.isNotEmpty(roleIds)) {
            Set<Long> allAffectedServiceIds = new HashSet<>();

            for (Long roleId : roleIds) {
                List<Long> affectedServiceIds = daoMgr.getXXPolicy().findServiceIdsByRoleId(roleId);
                allAffectedServiceIds.addAll(affectedServiceIds);
            }

            if (CollectionUtils.isNotEmpty(allAffectedServiceIds)) {
                for (final Long serviceId : allAffectedServiceIds) {
                    Runnable serviceVersionUpdater = new ServiceDBStore.ServiceVersionUpdater(daoMgr, serviceId, ServiceDBStore.VERSION_TYPE.POLICY_VERSION, null, RangerPolicyDelta.CHANGE_TYPE_SERVICE_CHANGE, null);
                    daoMgr.getRangerTransactionSynchronizationAdapter().executeOnTransactionCommit(serviceVersionUpdater);
                }
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("<== updatePolicyVersions(roleIds=" + roleIds + ")");
        }
    }

    private void updateRoleVersions(Set<Long> roleIds) {
        if (logger.isDebugEnabled()) {
            logger.debug("==> updatePolicyVersions(roleIds=" + roleIds + ")");
        }

        if (CollectionUtils.isNotEmpty(roleIds)) {
            Set<Long> allAffectedServiceIds = new HashSet<>();

            for (Long roleId : roleIds) {
                List<Long> affectedServiceIds = daoMgr.getXXPolicy().findServiceIdsByRoleId(roleId);
                allAffectedServiceIds.addAll(affectedServiceIds);
            }

            XXServiceDao serviceDao = daoMgr.getXXService();
            if (CollectionUtils.isNotEmpty(allAffectedServiceIds)) {
                for (final Long serviceId : allAffectedServiceIds) {
                    Runnable serviceVersionUpdater = new ServiceDBStore.ServiceVersionUpdater(daoMgr, serviceId, ServiceDBStore.VERSION_TYPE.ROLE_VERSION, null, RangerPolicyDelta.CHANGE_TYPE_ROLE_UPDATE, null);
                    daoMgr.getRangerTransactionSynchronizationAdapter().executeOnTransactionCommit(serviceVersionUpdater);
                    XXService serviceDbObj = serviceDao.getById(serviceId);
                    boolean   isTagService = serviceDbObj.getType() == EmbeddedServiceDefsUtil.instance().getTagServiceDefId();
                    if (isTagService) {
                        updateRoleVersionOfAllServicesRefferingTag(daoMgr, serviceDao, serviceId);
                    }
                }
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("<== updatePolicyVersions(roleIds=" + roleIds + ")");
        }
    }

    private void updateRoleVersionOfAllServicesRefferingTag(RangerDaoManager daoManager, XXServiceDao serviceDao, Long serviceId) {
        List<XXService> referringServices = serviceDao.findByTagServiceId(serviceId);
        if(CollectionUtils.isNotEmpty(referringServices)) {
            for(XXService referringService : referringServices) {
                final Long referringServiceId = referringService.getId();
                Runnable   roleVersionUpdater = new ServiceDBStore.ServiceVersionUpdater(daoManager, referringServiceId, ServiceDBStore.VERSION_TYPE.ROLE_VERSION, null, RangerPolicyDelta.CHANGE_TYPE_ROLE_UPDATE, null);
                daoMgr.getRangerTransactionSynchronizationAdapter().executeOnTransactionCommit(roleVersionUpdater);
            }
        }
    }
}

