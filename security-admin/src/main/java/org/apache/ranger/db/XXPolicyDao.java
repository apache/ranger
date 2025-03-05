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

package org.apache.ranger.db;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */

@Service
public class XXPolicyDao extends BaseDao<XXPolicy> {
    /**
     * Default Constructor
     */
    public XXPolicyDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public long getCountById(Long policyId) {
        return getEntityManager()
                .createNamedQuery("XXPolicy.countById", Long.class)
                .setParameter("policyId", policyId)
                .getSingleResult();
    }

    public XXPolicy findByNameAndServiceId(String polName, Long serviceId) {
        return findByNameAndServiceIdAndZoneId(polName, serviceId, RangerSecurityZone.RANGER_UNZONED_SECURITY_ZONE_ID);
    }

    public XXPolicy findByNameAndServiceIdAndZoneId(String polName, Long serviceId, Long zoneId) {
        if (polName == null || serviceId == null) {
            return null;
        }

        XXPolicy ret;

        try {
            ret = getEntityManager()
                    .createNamedQuery("XXPolicy.findByNameAndServiceIdAndZoneId", tClass)
                    .setParameter("polName", polName).setParameter("serviceId", serviceId).setParameter("zoneId", zoneId)
                    .getSingleResult();
        } catch (NoResultException e) {
            ret = null;
        }

        return ret;
    }

    public XXPolicy findByPolicyName(String polName) {
        if (polName == null) {
            return null;
        }

        try {
            return getEntityManager().createNamedQuery("XXPolicy.findByPolicyName", tClass)
                    .setParameter("polName", polName).getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    public List<XXPolicy> findByServiceId(Long serviceId) {
        if (serviceId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXPolicy.findByServiceId", tClass)
                    .setParameter("serviceId", serviceId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public List<Long> findPolicyIdsByServiceId(Long serviceId) {
        List<Long> ret;

        try {
            ret = getEntityManager()
                    .createNamedQuery("XXPolicy.findPolicyIdsByServiceId", Long.class)
                    .setParameter("serviceId", serviceId).getResultList();
        } catch (Exception e) {
            ret = new ArrayList<>();
        }

        return ret;
    }

    public Long getMaxIdOfXXPolicy() {
        try {
            return (Long) getEntityManager().createNamedQuery("XXPolicy.getMaxIdOfXXPolicy").getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    public List<XXPolicy> findByResourceSignatureByPolicyStatus(String serviceName, String policySignature, Boolean isPolicyEnabled) {
        if (policySignature == null || serviceName == null || isPolicyEnabled == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager().createNamedQuery("XXPolicy.findByResourceSignatureByPolicyStatus", tClass)
                    .setParameter("resSignature", policySignature)
                    .setParameter("serviceName", serviceName)
                    .setParameter("isPolicyEnabled", isPolicyEnabled)
                    .getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public List<XXPolicy> findByResourceSignature(String serviceName, String policySignature) {
        if (policySignature == null || serviceName == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager().createNamedQuery("XXPolicy.findByResourceSignature", tClass)
                    .setParameter("resSignature", policySignature)
                    .setParameter("serviceName", serviceName)
                    .getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public List<XXPolicy> findByServiceDefId(Long serviceDefId) {
        if (serviceDefId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager().createNamedQuery("XXPolicy.findByServiceDefId", tClass)
                    .setParameter("serviceDefId", serviceDefId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public void updateSequence() {
        Long maxId = getMaxIdOfXXPolicy();

        if (maxId == null) {
            return;
        }

        updateSequence("X_POLICY_SEQ", maxId + 1);
    }

    public List<XXPolicy> findByUserId(Long userId) {
        if (userId == null || userId.equals(0L)) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXPolicy.findByUserId", tClass)
                    .setParameter("userId", userId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public List<XXPolicy> findByGroupId(Long groupId) {
        if (groupId == null || groupId.equals(0L)) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXPolicy.findByGroupId", tClass)
                    .setParameter("groupId", groupId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public List<Long> findPolicyIdsByServiceNameAndZoneId(String serviceName, Long zoneId) {
        List<Long> ret;

        try {
            ret = getEntityManager()
                    .createNamedQuery("XXPolicy.findPolicyIdsByServiceNameAndZoneId", Long.class)
                    .setParameter("serviceName", serviceName)
                    .setParameter("zoneId", zoneId)
                    .getResultList();
        } catch (Exception e) {
            ret = new ArrayList<>();
        }

        return ret;
    }

    public List<XXPolicy> findByRoleId(Long roleId) {
        List<XXPolicy> ret = Collections.emptyList();

        if (roleId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXPolicy.findByRoleId", tClass)
                        .setParameter("roleId", roleId)
                        .getResultList();
            } catch (NoResultException excp) {
                // ignore
            }
        }

        return ret;
    }

    public List<Long> findServiceIdsByRoleId(Long roleId) {
        List<Long> ret = Collections.emptyList();

        if (roleId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXPolicy.findServiceIdsByRoleId", Long.class)
                        .setParameter("roleId", roleId)
                        .getResultList();
            } catch (NoResultException excp) {
                // ignore
            }
        }

        return ret;
    }

    public long findRoleRefPolicyCount(String roleName, Long serviceId) {
        try {
            return getEntityManager()
                    .createNamedQuery("XXPolicy.findRoleRefPolicyCount", Long.class)
                    .setParameter("serviceId", serviceId)
                    .setParameter("roleName", roleName).getSingleResult();
        } catch (Exception e) {
            // ignore
        }

        return -1;
    }

    public long getPoliciesCount(String serviceName) {
        try {
            return getEntityManager()
                    .createNamedQuery("XXPolicy.getPoliciesCount", Long.class)
                    .setParameter("serviceName", serviceName).getSingleResult();
        } catch (Exception e) {
            // ignore
        }

        return 0L;
    }

    public XXPolicy findPolicy(String policyName, String serviceName, String zoneName) {
        if (policyName == null || serviceName == null) {
            return null;
        }

        try {
            if (zoneName == null) {
                return getEntityManager().createNamedQuery("XXPolicy.findPolicyByPolicyNameAndServiceName", tClass)
                        .setParameter("policyName", policyName).setParameter("serviceName", serviceName)
                        .setParameter("zoneId", RangerSecurityZone.RANGER_UNZONED_SECURITY_ZONE_ID)
                        .getSingleResult();
            } else {
                return getEntityManager()
                        .createNamedQuery("XXPolicy.findPolicyByPolicyNameAndServiceNameAndZoneName", tClass)
                        .setParameter("policyName", policyName).setParameter("serviceName", serviceName)
                        .setParameter("zoneName", zoneName).getSingleResult();
            }
        } catch (NoResultException e) {
            return null;
        }
    }

    public List<XXPolicy> getAllByPolicyItem() {
        List<XXPolicy> ret;

        try {
            ret = getEntityManager().createNamedQuery("XXPolicy.getAllByPolicyItem", tClass)
                    .getResultList();
        } catch (NoResultException excp) {
            ret = Collections.emptyList();
        }

        return ret;
    }

    public XXPolicy findPolicyByGUIDAndServiceNameAndZoneName(String guid, String serviceName, String zoneName) {
        if (guid == null) {
            return null;
        }

        try {
            if (StringUtils.isNotBlank(serviceName)) {
                if (StringUtils.isNotBlank(zoneName)) {
                    return getEntityManager()
                            .createNamedQuery("XXPolicy.findPolicyByPolicyGUIDAndServiceNameAndZoneName", tClass)
                            .setParameter("guid", guid)
                            .setParameter("serviceName", serviceName)
                            .setParameter("zoneName", zoneName)
                            .getSingleResult();
                } else {
                    return getEntityManager().createNamedQuery("XXPolicy.findPolicyByPolicyGUIDAndServiceName", tClass)
                            .setParameter("guid", guid)
                            .setParameter("serviceName", serviceName)
                            .setParameter("zoneId", RangerSecurityZone.RANGER_UNZONED_SECURITY_ZONE_ID)
                            .getSingleResult();
                }
            } else {
                if (StringUtils.isNotBlank(zoneName)) {
                    return getEntityManager()
                            .createNamedQuery("XXPolicy.findPolicyByPolicyGUIDAndZoneName", tClass)
                            .setParameter("guid", guid)
                            .setParameter("zoneName", zoneName)
                            .getSingleResult();
                } else {
                    return getEntityManager()
                            .createNamedQuery("XXPolicy.findPolicyByPolicyGUID", tClass)
                            .setParameter("guid", guid)
                            .setParameter("zoneId", RangerSecurityZone.RANGER_UNZONED_SECURITY_ZONE_ID)
                            .getSingleResult();
                }
            }
        } catch (NoResultException e) {
            return null;
        }
    }

    public List<XXPolicy> findByPolicyStatus(Boolean isPolicyEnabled) {
        if (isPolicyEnabled == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager().createNamedQuery("XXPolicy.findByPolicyStatus", tClass)
                    .setParameter("isPolicyEnabled", isPolicyEnabled)
                    .getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public List<String> findDuplicateGUIDByServiceIdAndZoneId(Long serviceId, Long zoneId) {
        List<String> ret = Collections.emptyList();

        if (serviceId == null || zoneId == null) {
            return ret;
        }

        try {
            ret = getEntityManager().createNamedQuery("XXPolicy.findDuplicateGUIDByServiceIdAndZoneId", String.class)
                    .setParameter("serviceId", serviceId)
                    .setParameter("zoneId", zoneId)
                    .getResultList();
        } catch (Exception e) {
            // ignore
        }

        return ret;
    }

    public List<XXPolicy> findPolicyByGUIDAndServiceIdAndZoneId(String guid, Long serviceId, Long zoneId) {
        List<XXPolicy> ret = Collections.emptyList();

        if (guid == null || serviceId == null || zoneId == null) {
            return ret;
        }

        try {
            ret = getEntityManager().createNamedQuery("XXPolicy.findPolicyByGUIDAndServiceIdAndZoneId", tClass)
                    .setParameter("guid", guid)
                    .setParameter("serviceId", serviceId)
                    .setParameter("zoneId", zoneId)
                    .getResultList();
        } catch (NoResultException excp) {
            // ignore
        }

        return ret;
    }

    public Map<String, Long> findDuplicatePoliciesByServiceAndResourceSignature() {
        Map<String, Long> policies = new HashMap<>();

        try {
            List<Object[]> rows = getEntityManager().createNamedQuery("XXPolicy.findDuplicatePoliciesByServiceAndResourceSignature", Object[].class).getResultList();

            if (rows != null) {
                for (Object[] row : rows) {
                    policies.put((String) row[0], (Long) row[1]);
                }
            }
        } catch (NoResultException e) {
            return null;
        } catch (Exception ex) {
            // ignore
        }

        return policies;
    }

    public List<XXPolicy> findByServiceIdAndResourceSignature(Long serviceId, String policySignature) {
        if (policySignature == null || serviceId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager().createNamedQuery("XXPolicy.findByServiceIdAndResourceSignature", tClass)
                    .setParameter("serviceId", serviceId)
                    .setParameter("resSignature", policySignature)
                    .getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public List<XXPolicy> findByZoneId(Long zoneId) {
        if (zoneId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXPolicy.findByZoneId", tClass)
                    .setParameter("zoneId", zoneId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public List<XXPolicy> findByServiceType(String serviceType) {
        List<XXPolicy> ret = Collections.emptyList();

        if (serviceType != null && !serviceType.isEmpty()) {
            try {
                ret = getEntityManager().createNamedQuery("XXPolicy.findByServiceType", tClass)
                        .setParameter("serviceType", serviceType)
                        .getResultList();
            } catch (NoResultException e) {
                // ignore
            }
        }

        return ret;
    }

    public XXPolicy getProjectPolicy(Long projectId, Long policyId) {
        XXPolicy ret = null;

        if (projectId != null && policyId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXPolicy.getProjectPolicy", tClass)
                        .setParameter("projectId", projectId)
                        .setParameter("policyId", policyId)
                        .getSingleResult();
            } catch (NoResultException e) {
                // ignore
            }
        }

        return ret;
    }

    public List<Object[]> getMetaAttributesForPolicies(List<Long> policyIds) {
        if (policyIds == null || policyIds.isEmpty()) {
            return Collections.emptyList();
        }

        return getEntityManager().createNamedQuery("XXPolicy.getMetaAttributesForPolicies", Object[].class).setParameter("policyIds", policyIds).getResultList();
    }

    public List<XXPolicy> getProjectPolicies(Long projectId) {
        List<XXPolicy> ret = Collections.emptyList();

        if (projectId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXPolicy.getProjectPolicies", tClass)
                        .setParameter("projectId", projectId)
                        .getResultList();
            } catch (NoResultException e) {
                // ignore
            }
        }

        return ret;
    }
}
