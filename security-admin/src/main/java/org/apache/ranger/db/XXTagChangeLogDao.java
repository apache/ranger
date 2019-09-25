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

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXTagChangeLog;
import org.springframework.stereotype.Service;

/**
 */
@Service
public class XXTagChangeLogDao extends BaseDao<XXTagChangeLog> {

    private static final Log LOG = LogFactory.getLog(XXTagChangeLogDao.class);

    /**
     * Default Constructor
     */
    public XXTagChangeLogDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public List<XXTagChangeLog> findLaterThan(Long version, Long serviceId) {
        final List<XXTagChangeLog> ret;
        if (version != null) {
            List<Object[]> logs = getEntityManager()
                    .createNamedQuery("XXTagChangeLog.findSinceVersion", Object[].class)
                    .setParameter("version", version)
                    .setParameter("serviceId", serviceId)
                    .getResultList();
            // Ensure that some record has the same version as the base-version from where the records are fetched
            if (CollectionUtils.isNotEmpty(logs)) {
                Iterator<Object[]> iter = logs.iterator();
                boolean foundAndRemoved = false;

                while (iter.hasNext()) {
                    Object[] record = iter.next();
                    Long recordVersion = (Long) record[2];
                    if (version.equals(recordVersion)) {
                        iter.remove();
                        foundAndRemoved = true;
                    } else {
                        break;
                    }
                }
                if (foundAndRemoved) {
                    ret = convert(logs);
                } else {
                    ret = null;
                }
            } else {
                ret = null;
            }
        } else {
            ret = null;
        }

        return ret;
    }

    public void deleteOlderThan(int olderThanInDays) {

        Date since = new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(olderThanInDays));

        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting records from x_tag_change_log that are older than " + olderThanInDays + " days, that is,  older than " + since);
        }

        getEntityManager().createNamedQuery("XXTagChangeLog.deleteOlderThan").setParameter("olderThan", since).executeUpdate();
    }

    private List<XXTagChangeLog> convert(List<Object[]> queryResult) {

        final List<XXTagChangeLog> ret;

        if (CollectionUtils.isNotEmpty(queryResult)) {

            ret = new ArrayList<>(queryResult.size());

            for (Object[] log : queryResult) {

                Long logRecordId = (Long) log[0];
                Integer tagChangeType = (Integer) log[1];
                Long serviceTagsVersion = (Long) log[2];
                Long serviceResourceId = (Long) log[3];
                Long tagId = (Long) log[4];

                ret.add(new XXTagChangeLog(logRecordId, tagChangeType, serviceTagsVersion, serviceResourceId, tagId));
            }
        } else {
            ret = null;
        }
        return ret;

    }

}

