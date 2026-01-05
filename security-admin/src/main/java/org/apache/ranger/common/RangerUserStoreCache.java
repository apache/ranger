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

package org.apache.ranger.common;

import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.plugin.model.GroupInfo;
import org.apache.ranger.plugin.model.UserInfo;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class RangerUserStoreCache {
    private static final Logger LOG = LoggerFactory.getLogger(RangerUserStoreCache.class);

    private static final int MAX_WAIT_TIME_FOR_UPDATE = 10;

    public static volatile RangerUserStoreCache sInstance;

    private final int             waitTimeInSeconds;
    private final boolean         dedupStrings;
    private final ReentrantLock   lock = new ReentrantLock();
    private       RangerUserStore rangerUserStore;

    private RangerUserStoreCache() {
        RangerAdminConfig config = RangerAdminConfig.getInstance();

        this.waitTimeInSeconds = config.getInt("ranger.admin.userstore.download.cache.max.waittime.for.update", MAX_WAIT_TIME_FOR_UPDATE);
        this.dedupStrings      = config.getBoolean("ranger.admin.userstore.dedup.strings", Boolean.TRUE);
        this.rangerUserStore   = new RangerUserStore();
    }

    public static RangerUserStoreCache getInstance() {
        RangerUserStoreCache me = sInstance;

        if (me == null) {
            synchronized (RangerUserStoreCache.class) {
                me = sInstance;

                if (me == null) {
                    me        = new RangerUserStoreCache();
                    sInstance = me;
                }
            }
        }

        return me;
    }

    public RangerUserStore getRangerUserStore() {
        return this.rangerUserStore;
    }

    public RangerUserStore getLatestRangerUserStoreOrCached(XUserMgr xUserMgr) {
        LOG.debug("==> RangerUserStoreCache.getLatestRangerUserStoreOrCached()");

        RangerUserStore ret;
        boolean         lockResult = false;

        try {
            lockResult = lock.tryLock(waitTimeInSeconds, TimeUnit.SECONDS);

            if (lockResult) {
                Long cachedUserStoreVersion = rangerUserStore.getUserStoreVersion();
                Long dbUserStoreVersion     = xUserMgr.getUserStoreVersion();

                if (!Objects.equals(cachedUserStoreVersion, dbUserStoreVersion)) {
                    LOG.info("RangerUserStoreCache refreshing from version {} to {}", cachedUserStoreVersion, dbUserStoreVersion);

                    final long                     startTimeMs      = System.currentTimeMillis();
                    final Set<UserInfo>            rangerUsersInDB  = xUserMgr.getUsers();
                    final Set<GroupInfo>           rangerGroupsInDB = xUserMgr.getGroups();
                    final Map<String, Set<String>> userGroups       = xUserMgr.getUserGroups();
                    final long                     dbLoadTime       = System.currentTimeMillis() - startTimeMs;

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("No. of users from DB = {} and no. of groups from DB = {}", rangerUsersInDB.size(), rangerGroupsInDB.size());
                        LOG.debug("No. of userGroupMappings = {}", userGroups.size());
                        LOG.debug("loading Users from database and it took:{} seconds", TimeUnit.MILLISECONDS.toSeconds(dbLoadTime));
                    }

                    RangerUserStore rangerUserStore = new RangerUserStore(dbUserStoreVersion, rangerUsersInDB, rangerGroupsInDB, userGroups);

                    if (dedupStrings) {
                        rangerUserStore.dedupStrings();
                    }

                    this.rangerUserStore = rangerUserStore;

                    LOG.info("RangerUserStoreCache refreshed from version {} to {}: users={}, groups={}, userGroupMappings={}", cachedUserStoreVersion, dbUserStoreVersion, rangerUsersInDB.size(), rangerGroupsInDB.size(), userGroups.size());
                }
            } else {
                LOG.debug("Could not get lock in [{}] seconds, returning cached RangerUserStore", waitTimeInSeconds);
            }
        } catch (InterruptedException exception) {
            LOG.error("RangerUserStoreCache.getLatestRangerUserStoreOrCached:lock got interrupted..", exception);
        } finally {
            ret = rangerUserStore;

            if (lockResult) {
                lock.unlock();
            }
        }

        LOG.debug("<== RangerUserStoreCache.getLatestRangerUserStoreOrCached(): ret={}", ret);

        return ret;
    }
}
