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

package org.apache.ranger.db;

import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPortalUser;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
public class XXPortalUserDao extends BaseDao<XXPortalUser> {
    public XXPortalUserDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public XXPortalUser findByLoginId(String loginId) {
        if (daoManager.getStringUtil().isEmpty(loginId)) {
            return null;
        }

        List<XXPortalUser> resultList = getEntityManager()
                .createNamedQuery("XXPortalUser.findByLoginId", tClass)
                .setParameter("loginId", loginId).getResultList();

        if (!resultList.isEmpty()) {
            return resultList.get(0);
        }

        return null;
    }

    public XXPortalUser findByEmailAddress(String emailAddress) {
        if (daoManager.getStringUtil().isEmpty(emailAddress)) {
            return null;
        }

        List<XXPortalUser> resultList = getEntityManager()
                .createNamedQuery("XXPortalUser.findByEmailAddress", tClass)
                .setParameter("emailAddress", emailAddress)
                .getResultList();

        if (!resultList.isEmpty()) {
            return resultList.get(0);
        }

        return null;
    }

    public List<XXPortalUser> findByRole(String userRole) {
        return getEntityManager().createNamedQuery("XXPortalUser.findByRole", tClass)
                .setParameter("userRole", userRole.toUpperCase())
                .getResultList();
    }

    public List<Object[]> getUserAddedReport() {
        return getEntityManager()
                .createNamedQuery("XXPortalUser.getUserAddedReport", Object[].class)
                .getResultList();
    }

    public XXPortalUser findByXUserId(Long xUserId) {
        if (xUserId == null) {
            return null;
        }

        try {
            return getEntityManager().createNamedQuery("XXPortalUser.findByXUserId", tClass)
                    .setParameter("id", xUserId).getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    public List<XXPortalUser> findAllXPortalUser() {
        try {
            return getEntityManager().createNamedQuery("XXPortalUser.findAllXPortalUser", tClass).getResultList();
        } catch (Exception e) {
            return null;
        }
    }

    public List<String> getNonUserRoleExternalUsers() {
        try {
            return getEntityManager().createNamedQuery("XXPortalUser.getNonUserRoleExternalUsers", String.class)
                    .setParameter("userRole", RangerConstants.ROLE_USER)
                    .setParameter("userSource", RangerCommonEnums.USER_EXTERNAL)
                    .getResultList();
        } catch (Exception e) {
            return null;
        }
    }

    public List<XXPortalUser> findByUserSourceAndStatus(final int source, final int status) {
        try {
            return getEntityManager().createNamedQuery("XXPortalUser.findByUserSourceAndStatus", tClass)
                    .setParameter("userSource", source)
                    .setParameter("status", status)
                    .getResultList();
        } catch (Exception e) {
            return null;
        }
    }

    public XXPortalUser findById(Long id) {
        XXPortalUser xXPortalUser;

        if (id == null) {
            return null;
        }

        try {
            xXPortalUser = new XXPortalUser();

            Object[] row = getEntityManager().createNamedQuery("XXPortalUser.findById", Object[].class).setParameter("id", id).getSingleResult();

            if (row != null) {
                xXPortalUser.setFirstName((String) row[0]);
                xXPortalUser.setLastName((String) row[1]);
                xXPortalUser.setPublicScreenName((String) row[2]);
                xXPortalUser.setLoginId((String) row[3]);

                return xXPortalUser;
            }
        } catch (NoResultException e) {
            return null;
        }

        return xXPortalUser;
    }

    public Map<String, Long> getCountByUserRole() {
        Map<String, Long> ret  = Collections.emptyMap();
        List<Object[]>    rows = getEntityManager().createNamedQuery("XXPortalUser.getCountByUserRole", Object[].class).getResultList();

        if (rows != null) {
            ret = new HashMap<>();

            for (Object[] row : rows) {
                if (Objects.nonNull(row) && Objects.nonNull(row[0]) && Objects.nonNull(row[1]) && (!row[0].toString().isEmpty())) {
                    // since group by query will not return empty count field, no need to check
                    ret.put((String) row[0], (Long) row[1]);
                }
            }
        }

        return ret;
    }
}
