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

import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXAuthSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class XXAuthSessionDao extends BaseDao<XXAuthSession> {
    private static final Logger LOG = LoggerFactory.getLogger(XXAuthSessionDao.class);

    public XXAuthSessionDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public List<Object[]> getUserLoggedIn() {
        return getEntityManager()
                .createNamedQuery("XXAuthSession.getUserLoggedIn", Object[].class)
                .getResultList();
    }

    public XXAuthSession getAuthSessionBySessionId(String sessionId) {
        try {
            return (XXAuthSession) getEntityManager()
                    .createNamedQuery("XXAuthSession.getAuthSessionBySessionId")
                    .setParameter("sessionId", sessionId)
                    .getSingleResult();
        } catch (NoResultException ignoreNoResultFound) {
            return null;
        }
    }

    public List<XXAuthSession> getAuthSessionByUserId(Long userId) {
        try {
            return getEntityManager()
                    .createNamedQuery("XXAuthSession.getAuthSessionByUserId", tClass)
                    .setParameter("userId", userId)
                    .getResultList();
        } catch (NoResultException ignoreNoResultFound) {
            return null;
        }
    }

    public long getRecentAuthFailureCountByLoginId(String loginId, int timeRangezSecond) {
        Date utcDate             = DateUtil.getUTCDate();
        Date authWindowStartTime = new Date((utcDate != null ? utcDate.getTime() : System.currentTimeMillis()) - timeRangezSecond * 1000L);

        return getEntityManager()
                .createNamedQuery("XXAuthSession.getRecentAuthFailureCountByLoginId", Long.class)
                .setParameter("loginId", loginId)
                .setParameter("authWindowStartTime", authWindowStartTime)
                .getSingleResult();
    }

    public List<Long> getAuthSessionIdsByUserId(Long userId) {
        if (userId == null) {
            return null;
        }

        return getEntityManager()
                .createNamedQuery("XXAuthSession.findIdsByUserId", Long.class)
                .setParameter("userId", userId)
                .getResultList();
    }

    public void deleteAuthSessionsByIds(List<Long> ids) {
        batchDeleteByIds("XXAuthSession.deleteByIds", ids, "ids");
    }

    public long deleteOlderThan(int olderThanInDays) {
        Date since = new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(olderThanInDays));

        LOG.info("Deleting x_auth_sess records that are older than {} days, that is, older than {}", olderThanInDays, since);
        long ret = getEntityManager().createNamedQuery("XXAuthSession.deleteOlderThan").setParameter("olderThan", since).executeUpdate();
        LOG.info("Deleted {} x_auth_sess records", ret);

        LOG.info("Updating x_trx_log_v2.sess_id with null which are older than {} days, that is, older than {}", olderThanInDays, since);
        long updated = getEntityManager().createNamedQuery("XXTrxLogV2.updateSessIdWithNull").setParameter("olderThan", since).executeUpdate();
        LOG.info("Updated {} x_trx_log_v2 records", updated);
        return ret;
    }
}
