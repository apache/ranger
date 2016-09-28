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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.store.TagStore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.util.ServiceTags;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class RangerServiceTagsCache {
	private static final Log LOG = LogFactory.getLog(RangerServiceTagsCache.class);

	private static final int MAX_WAIT_TIME_FOR_UPDATE = 10;

	private static volatile RangerServiceTagsCache sInstance = null;
	private final boolean useServiceTagsCache;
	private final int waitTimeInSeconds;

	private final Map<String, ServiceTagsWrapper> serviceTagsMap = new HashMap<String, ServiceTagsWrapper>();

	public static RangerServiceTagsCache getInstance() {
		if (sInstance == null) {
			synchronized (RangerServiceTagsCache.class) {
				if (sInstance == null) {
					sInstance = new RangerServiceTagsCache();
				}
			}
		}
		return sInstance;
	}

	private RangerServiceTagsCache() {
		useServiceTagsCache = RangerConfiguration.getInstance().getBoolean("ranger.admin.tag.download.usecache", true);
		waitTimeInSeconds = RangerConfiguration.getInstance().getInt("ranger.admin.tag.download.cache.max.waittime.for.update", MAX_WAIT_TIME_FOR_UPDATE);
	}

	public void dump() {

		if (useServiceTagsCache) {
			Set<String> serviceNames = null;

			synchronized (this) {
				serviceNames = serviceTagsMap.keySet();
			}

			if (CollectionUtils.isNotEmpty(serviceNames)) {
				ServiceTagsWrapper cachedServiceTagsWrapper = null;

				for (String serviceName : serviceNames) {
					synchronized (this) {
						cachedServiceTagsWrapper = serviceTagsMap.get(serviceName);
					}
					if (LOG.isDebugEnabled()) {
						LOG.debug("serviceName:" + serviceName + ", Cached-MetaData:" + cachedServiceTagsWrapper);
					}
				}
			}
		}
	}

	public ServiceTags getServiceTags(String serviceName, Long serviceId, TagStore tagStore) throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceTagsCache.getServiceTags(" + serviceName + ", " + serviceId + ")");
		}

		ServiceTags ret = null;

		if (StringUtils.isNotBlank(serviceName) && serviceId != null) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("useServiceTagsCache=" + useServiceTagsCache);
			}

			ServiceTags serviceTags = null;

			if (!useServiceTagsCache) {
				if (tagStore != null) {
					try {
						serviceTags = tagStore.getServiceTags(serviceName);
					} catch (Exception exception) {
						LOG.error("getServiceTags(" + serviceName + "): failed to get latest tags from tag-store", exception);
					}
				} else {
					LOG.error("getServiceTags(" + serviceName + "): failed to get latest tags as tag-store is null!");
				}
			} else {
				ServiceTagsWrapper serviceTagsWrapper = null;

				synchronized (this) {
					serviceTagsWrapper = serviceTagsMap.get(serviceName);

					if (serviceTagsWrapper != null) {
						if (!serviceId.equals(serviceTagsWrapper.getServiceId())) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Service [" + serviceName + "] changed service-id from " + serviceTagsWrapper.getServiceId()
										+ " to " + serviceId);
								LOG.debug("Recreating serviceTagsWrapper for serviceName [" + serviceName + "]");
							}
							serviceTagsMap.remove(serviceName);
							serviceTagsWrapper = null;
						}
					}
					if (serviceTagsWrapper == null) {
						serviceTagsWrapper = new ServiceTagsWrapper(serviceId);
						serviceTagsMap.put(serviceName, serviceTagsWrapper);
					}
				}

				if (tagStore != null) {
					boolean refreshed = serviceTagsWrapper.getLatestOrCached(serviceName, tagStore);

					if(LOG.isDebugEnabled()) {
						LOG.debug("getLatestOrCached returned " + refreshed);
					}
				} else {
					LOG.error("getServiceTags(" + serviceName + "): failed to get latest tags as tag-store is null!");
				}

				serviceTags = serviceTagsWrapper.getServiceTags();
			}

			ret = serviceTags;

		} else {
			LOG.error("getServiceTags() failed to get tags as serviceName is null or blank and/or serviceId is null!");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceTagsCache.getServiceTags(" + serviceName + ", " + serviceId + "): count=" + ((ret == null || ret.getTags() == null) ? 0 : ret.getTags().size()));
		}

		return ret;
	}

	private class ServiceTagsWrapper {
		final Long serviceId;
		ServiceTags serviceTags;
		Date updateTime = null;
		long longestDbLoadTimeInMs = -1;

		ReentrantLock lock = new ReentrantLock();

		ServiceTagsWrapper(Long serviceId) {
			this.serviceId = serviceId;
			serviceTags = null;
		}

		Long getServiceId() { return serviceId; }

		ServiceTags getServiceTags() {
			return serviceTags;
		}

		Date getUpdateTime() {
			return updateTime;
		}

		long getLongestDbLoadTimeInMs() {
			return longestDbLoadTimeInMs;
		}

		boolean getLatestOrCached(String serviceName, TagStore tagStore) throws Exception {
			boolean ret = false;

			try {
				ret = lock.tryLock(waitTimeInSeconds, TimeUnit.SECONDS);
				if (ret) {
					getLatest(serviceName, tagStore);
				}
			} catch (InterruptedException exception) {
				LOG.error("getLatestOrCached:lock got interrupted..", exception);
			} finally {
				if (ret) {
					lock.unlock();
				}
			}

			return ret;
		}

		void getLatest(String serviceName, TagStore tagStore) throws Exception {

			if (LOG.isDebugEnabled()) {
				LOG.debug("==> ServiceTagsWrapper.getLatest(" + serviceName + ")");
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("Found ServiceTags in-cache : " + (serviceTags != null));
			}

			Long tagVersionInDb = tagStore.getTagVersion(serviceName);


			if (serviceTags == null || tagVersionInDb == null || !tagVersionInDb.equals(serviceTags.getTagVersion())) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("loading serviceTags from db ... cachedServiceTagsVersion=" + (serviceTags != null ? serviceTags.getTagVersion() : null) + ", tagVersionInDb=" + tagVersionInDb);
				}

				long startTimeMs = System.currentTimeMillis();

				ServiceTags serviceTagsFromDb = tagStore.getServiceTags(serviceName);

				long dbLoadTime = System.currentTimeMillis() - startTimeMs;

				if (dbLoadTime > longestDbLoadTimeInMs) {
					longestDbLoadTimeInMs = dbLoadTime;
				}
				updateTime = new Date();

				if (serviceTagsFromDb != null) {
					if (serviceTagsFromDb.getTagVersion() == null) {
						serviceTagsFromDb.setTagVersion(0L);
					}
					serviceTags = serviceTagsFromDb;
					pruneUnusedAttributes();
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== ServiceTagsWrapper.getLatest(" + serviceName + ")");
			}
		}

		private void pruneUnusedAttributes() {
			if (serviceTags != null) {
				serviceTags.setOp(null);
				serviceTags.setTagUpdateTime(null);

				serviceTags.setTagDefinitions(null);

				for (Map.Entry<Long, RangerTag> entry : serviceTags.getTags().entrySet()) {
					RangerTag tag = entry.getValue();
					tag.setCreatedBy(null);
					tag.setCreateTime(null);
					tag.setUpdatedBy(null);
					tag.setUpdateTime(null);
					tag.setGuid(null);
				}

				for (RangerServiceResource serviceResource : serviceTags.getServiceResources()) {
					serviceResource.setCreatedBy(null);
					serviceResource.setCreateTime(null);
					serviceResource.setUpdatedBy(null);
					serviceResource.setUpdateTime(null);
					serviceResource.setGuid(null);

					serviceResource.setServiceName(null);
					serviceResource.setResourceSignature(null);
				}
			}
		}

		StringBuilder toString(StringBuilder sb) {
			sb.append("RangerServiceTagsWrapper={");

			sb.append("updateTime=").append(updateTime)
					.append(", longestDbLoadTimeInMs=").append(longestDbLoadTimeInMs)
					.append(", Service-Version:").append(serviceTags != null ? serviceTags.getTagVersion() : "null")
					.append(", Number-Of-Tags:").append(serviceTags != null ? serviceTags.getTags().size() : 0);

			sb.append("} ");

			return sb;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}
	}
}

