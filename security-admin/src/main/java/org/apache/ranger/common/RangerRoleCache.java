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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.biz.RoleDBStore;
import org.apache.ranger.plugin.model.RangerRole;

import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.SearchFilter;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class RangerRoleCache {
	private static final Log LOG = LogFactory.getLog(RangerRoleCache.class);

	private static final int MAX_WAIT_TIME_FOR_UPDATE = 10;

	public static volatile RangerRoleCache sInstance = null;
	private final int waitTimeInSeconds;
	final ReentrantLock lock = new ReentrantLock();

	RangerRoleCacheWrapper rangerRoleCacheWrapper = null;

	public static RangerRoleCache getInstance() {
		if (sInstance == null) {
			synchronized (RangerRoleCache.class) {
				if (sInstance == null) {
					sInstance = new RangerRoleCache();
				}
			}
		}
		return sInstance;
	}

	private RangerRoleCache() {
		RangerAdminConfig config = new RangerAdminConfig();

		waitTimeInSeconds = config.getInt("ranger.admin.policy.download.cache.max.waittime.for.update", MAX_WAIT_TIME_FOR_UPDATE);
	}

	public RangerRoles getLatestRangerRoleOrCached(String serviceName, RoleDBStore roleDBStore, Long lastKnownRoleVersion, Long rangerRoleVersionInDB) throws Exception {
		RangerRoles ret = null;

		if (lastKnownRoleVersion == null || !lastKnownRoleVersion.equals(rangerRoleVersionInDB)) {
			rangerRoleCacheWrapper = new RangerRoleCacheWrapper();
			ret = rangerRoleCacheWrapper.getLatestRangerRoles(serviceName, roleDBStore, lastKnownRoleVersion, rangerRoleVersionInDB);
		} else if (lastKnownRoleVersion.equals(rangerRoleVersionInDB)) {
			ret = null;
		} else {
			ret = rangerRoleCacheWrapper.getRangerRoles();
		}

		return ret;
	}

	private class RangerRoleCacheWrapper {
		RangerRoles rangerRoles;
		Long 			rangerRoleVersion;

		RangerRoleCacheWrapper() {
			this.rangerRoles = null;
			this.rangerRoleVersion = -1L;
		}

		public RangerRoles getRangerRoles() {
			return this.rangerRoles;
		}

		public Long getRangerRoleVersion() {
			return this.rangerRoleVersion;
		}

		public RangerRoles getLatestRangerRoles(String serviceName, RoleDBStore roleDBStore, Long lastKnownRoleVersion, Long rangerRoleVersionInDB) throws Exception {
			RangerRoles ret	 = null;
			boolean         lockResult   = false;
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> RangerRoleCache.getLatestRangerRoles(ServiceName= " + serviceName + " lastKnownRoleVersion= " + lastKnownRoleVersion + " rangerRoleVersionInDB= " + rangerRoleVersionInDB + ")");
			}

			try {
				lockResult = lock.tryLock(waitTimeInSeconds, TimeUnit.SECONDS);

				if (lockResult) {
					// We are getting all the Roles to be downloaded for now. Should do downloades for each service based on what roles are there in the policies.
					SearchFilter searchFilter = null;
					final Set<RangerRole> rangerRoleInDB = new HashSet<>(roleDBStore.getRoles(searchFilter));

					Date updateTime = new Date();

					if (rangerRoleInDB != null) {
						ret = new RangerRoles();
						ret.setRangerRoles(rangerRoleInDB);
						ret.setRoleUpdateTime(updateTime);
						ret.setRoleVersion(rangerRoleVersionInDB);
						rangerRoleVersion = rangerRoleVersionInDB;
					} else {
						LOG.error("Could not get Ranger Roles from database ...");
					}
				} else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Could not get lock in [" + waitTimeInSeconds + "] seconds, returning cached RangerRoles");
					}
					ret = getRangerRoles();
				}
			} catch (InterruptedException exception) {
				LOG.error("RangerRoleCache.getLatestRangerRoles:lock got interrupted..", exception);
			} finally {
				if (lockResult) {
					lock.unlock();
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== RangerRoleCache.getLatestRangerRoles(ServiceName= " + serviceName + " lastKnownRoleVersion= " + lastKnownRoleVersion + " rangerRoleVersionInDB= " + rangerRoleVersionInDB + " RangerRoles= " + ret + ")");
			}
			return ret;
		}
	}
}

