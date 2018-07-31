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

package org.apache.ranger.biz;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.*;
import org.apache.ranger.plugin.model.*;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;


public class RangerTagDBRetriever {
	static final Log LOG = LogFactory.getLog(RangerTagDBRetriever.class);
	static final Log PERF_LOG = RangerPerfTracer.getPerfLogger("db.RangerTagDBRetriever");
	public static final String OPTION_RANGER_FILTER_TAGS_FOR_SERVICE_PLUGIN = "ranger.filter.tags.for.service.plugin";

	private final RangerDaoManager daoMgr;
	private final XXService xService;
	private final LookupCache lookupCache;

	private final PlatformTransactionManager  txManager;
	private final TransactionTemplate         txTemplate;

	private List<RangerServiceResource> serviceResources;
	private Map<Long, RangerTagDef> tagDefs;
	private Map<Long, RangerTag> tags;
	private List<RangerTagResourceMap> tagResourceMaps;

	private boolean filterForServicePlugin;

	public RangerTagDBRetriever(final RangerDaoManager daoMgr, final PlatformTransactionManager txManager, final XXService xService) {
		this.daoMgr = daoMgr;
		this.txManager = txManager;
		if (this.txManager != null) {
			this.txTemplate = new TransactionTemplate(this.txManager);
			this.txTemplate.setReadOnly(true);
		} else {
			this.txTemplate = null;
		}
		this.xService = xService;
		this.lookupCache = new LookupCache();


		if (this.daoMgr != null && this.xService != null) {

			RangerPerfTracer perf = null;

			if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "RangerTagDBReceiver.getTags(serviceName=" + xService.getName());
			}

			filterForServicePlugin = RangerConfiguration.getInstance().getBoolean(OPTION_RANGER_FILTER_TAGS_FOR_SERVICE_PLUGIN, false);

			if (this.txTemplate == null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Load Tags in the same thread and using an existing transaction");
				}
				if (initializeTagCache(xService) == false) {
					LOG.error("Failed to get tags for service:[" + xService.getName() + "] in the same thread and using an existing transaction");
				}
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Load Tags in a separate thread and using a new transaction");
				}

				TagLoaderThread t = new TagLoaderThread(txTemplate, xService);
				t.start();
				try {
					t.join();
				} catch (InterruptedException ie) {
					LOG.error("Failed to get Tags in a separate thread and using a new transaction", ie);
				}
			}

			RangerPerfTracer.log(perf);

		}
	}

	public List<RangerTagResourceMap> getTagResourceMaps() {
		return tagResourceMaps;
	}

	public List<RangerServiceResource> getServiceResources() {
		return serviceResources;
	}

	public Map<Long, RangerTagDef> getTagDefs() {
		return tagDefs;
	}

	public Map<Long, RangerTag> getTags() {
		return tags;
	}

	private boolean initializeTagCache(XXService xService) {
		boolean ret;
		try {
			TagRetrieverServiceResourceContext  serviceResourceContext  = new TagRetrieverServiceResourceContext(xService);
			TagRetrieverTagDefContext           tagDefContext           = new TagRetrieverTagDefContext(xService);
			TagRetrieverTagContext              tagContext              = new TagRetrieverTagContext(xService);

			serviceResources    = serviceResourceContext.getAllServiceResources();
			tagDefs             = tagDefContext.getAllTagDefs();
			tags                = tagContext.getAllTags();

			tagResourceMaps     = getAllTagResourceMaps();

			ret = true;
		} catch (Exception ex) {
			LOG.error("Failed to get tags for service:[" + xService.getName() + "]");
			serviceResources    = null;
			tagDefs             = null;
			tags                = null;
			tagResourceMaps     = null;
			ret = false;
		}
		return ret;
	}
	private List<RangerTagResourceMap> getAllTagResourceMaps() {

		List<XXTagResourceMap> xTagResourceMaps = filterForServicePlugin ? daoMgr.getXXTagResourceMap().findForServicePlugin(xService.getId()) : daoMgr.getXXTagResourceMap().findByServiceId(xService.getId());

		ListIterator<XXTagResourceMap> iterTagResourceMap = xTagResourceMaps.listIterator();

		List<RangerTagResourceMap> ret = new ArrayList<RangerTagResourceMap>();

		while (iterTagResourceMap.hasNext()) {

			XXTagResourceMap xTagResourceMap = iterTagResourceMap.next();

			if (xTagResourceMap != null) {

				RangerTagResourceMap tagResourceMap = new RangerTagResourceMap();

				tagResourceMap.setId(xTagResourceMap.getId());
				tagResourceMap.setGuid(xTagResourceMap.getGuid());
				tagResourceMap.setCreatedBy(lookupCache.getUserScreenName(xTagResourceMap.getAddedByUserId()));
				tagResourceMap.setUpdatedBy(lookupCache.getUserScreenName(xTagResourceMap.getUpdatedByUserId()));
				tagResourceMap.setCreateTime(xTagResourceMap.getCreateTime());
				tagResourceMap.setUpdateTime(xTagResourceMap.getUpdateTime());
				tagResourceMap.setResourceId(xTagResourceMap.getResourceId());
				tagResourceMap.setTagId(xTagResourceMap.getTagId());

				ret.add(tagResourceMap);
			}
		}
		return ret;
	}

	static <T> List<T> asList(T obj) {
		List<T> ret = new ArrayList<T>();

		if (obj != null) {
			ret.add(obj);
		}

		return ret;
	}

	private class LookupCache {
		final Map<Long, String> userScreenNames = new HashMap<Long, String>();
		final Map<Long, String> resourceDefs = new HashMap<Long, String>();

		String getUserScreenName(Long userId) {
			String ret = null;

			if (userId != null) {
				ret = userScreenNames.get(userId);

				if (ret == null) {
					XXPortalUser user = daoMgr.getXXPortalUser().getById(userId);

					if (user != null) {
						ret = user.getPublicScreenName();

						if (StringUtil.isEmpty(ret)) {
							ret = user.getFirstName();

							if (StringUtil.isEmpty(ret)) {
								ret = user.getLoginId();
							} else {
								if (!StringUtil.isEmpty(user.getLastName())) {
									ret += (" " + user.getLastName());
								}
							}
						}

						if (ret != null) {
							userScreenNames.put(userId, ret);
						}
					}
				}
			}

			return ret;
		}

		String getResourceName(Long resourceDefId) {
			String ret = null;

			if (resourceDefId != null) {
				ret = resourceDefs.get(resourceDefId);

				if (ret == null) {
					XXResourceDef xResourceDef = daoMgr.getXXResourceDef().getById(resourceDefId);

					if (xResourceDef != null) {
						ret = xResourceDef.getName();

						resourceDefs.put(resourceDefId, ret);
					}
				}
			}

			return ret;
		}
	}

	private class TagLoaderThread extends Thread {
		final TransactionTemplate txTemplate;
		final XXService           xService;

		TagLoaderThread(TransactionTemplate txTemplate, final XXService xService) {
			this.txTemplate = txTemplate;
			this.xService   = xService;
		}

		@Override
		public void run() {
			try {
				 Boolean result = txTemplate.execute(new TransactionCallback<Boolean>() {
					@Override
					public Boolean doInTransaction(TransactionStatus status) {
						boolean ret = initializeTagCache(xService);
						if (!ret) {
							status.setRollbackOnly();
							LOG.error("Failed to get tags for service:[" + xService.getName() + "] in a new transaction");
						}
						return ret;
					}
				});
				 if (LOG.isDebugEnabled()) {
				 	LOG.debug("transaction result:[" + result +"]");
				 }
			} catch (Throwable ex) {
				LOG.error("Failed to get tags for service:[" + xService.getName() + "] in a new transaction", ex);
			}
		}
	}

	private class TagRetrieverServiceResourceContext {

		final XXService service;
		final ListIterator<XXServiceResource> iterServiceResource;
		final ListIterator<XXServiceResourceElement> iterServiceResourceElement;
		final ListIterator<XXServiceResourceElementValue> iterServiceResourceElementValue;

		TagRetrieverServiceResourceContext(XXService xService) {
			Long serviceId = xService == null ? null : xService.getId();

			List<XXServiceResource> xServiceResources = filterForServicePlugin ? daoMgr.getXXServiceResource().findForServicePlugin(serviceId) : daoMgr.getXXServiceResource().findTaggedResourcesInServiceId(serviceId);
			List<XXServiceResourceElement> xServiceResourceElements = filterForServicePlugin ? daoMgr.getXXServiceResourceElement().findForServicePlugin(serviceId) : daoMgr.getXXServiceResourceElement().findTaggedResourcesInServiceId(serviceId);
			List<XXServiceResourceElementValue> xServiceResourceElementValues = filterForServicePlugin ? daoMgr.getXXServiceResourceElementValue().findForServicePlugin(serviceId) : daoMgr.getXXServiceResourceElementValue().findTaggedResourcesInServiceId(serviceId);

			this.service = xService;
			this.iterServiceResource = xServiceResources.listIterator();
			this.iterServiceResourceElement = xServiceResourceElements.listIterator();
			this.iterServiceResourceElementValue = xServiceResourceElementValues.listIterator();

		}

		TagRetrieverServiceResourceContext(XXServiceResource xServiceResource, XXService xService) {
			Long resourceId = xServiceResource == null ? null : xServiceResource.getId();

			List<XXServiceResource> xServiceResources = asList(xServiceResource);
			List<XXServiceResourceElement> xServiceResourceElements = daoMgr.getXXServiceResourceElement().findByResourceId(resourceId);
			List<XXServiceResourceElementValue> xServiceResourceElementValues = daoMgr.getXXServiceResourceElementValue().findByResourceId(resourceId);

			this.service = xService;
			this.iterServiceResource = xServiceResources.listIterator();
			this.iterServiceResourceElement = xServiceResourceElements.listIterator();
			this.iterServiceResourceElementValue = xServiceResourceElementValues.listIterator();

		}

		List<RangerServiceResource> getAllServiceResources() {
			List<RangerServiceResource> ret = new ArrayList<RangerServiceResource>();

			while (iterServiceResource.hasNext()) {
				RangerServiceResource serviceResource = getNextServiceResource();

				if (serviceResource != null) {
					ret.add(serviceResource);
				}
			}

			if (!hasProcessedAll()) {
				LOG.warn("getAllServiceResources(): perhaps one or more serviceResources got updated during retrieval. Using fallback ... ");

				ret = getServiceResourcesBySecondary();
			}

			return ret;
		}

		RangerServiceResource getNextServiceResource() {
			RangerServiceResource ret = null;

			if (iterServiceResource.hasNext()) {
				XXServiceResource xServiceResource = iterServiceResource.next();

				if (xServiceResource != null) {
					ret = new RangerServiceResource();

					ret.setId(xServiceResource.getId());
					ret.setGuid(xServiceResource.getGuid());
					ret.setIsEnabled(xServiceResource.getIsEnabled());
					ret.setCreatedBy(lookupCache.getUserScreenName(xServiceResource.getAddedByUserId()));
					ret.setUpdatedBy(lookupCache.getUserScreenName(xServiceResource.getUpdatedByUserId()));
					ret.setCreateTime(xServiceResource.getCreateTime());
					ret.setUpdateTime(xServiceResource.getUpdateTime());
					ret.setVersion(xServiceResource.getVersion());
					ret.setResourceSignature(xServiceResource.getResourceSignature());

					getServiceResourceElements(ret);
				}
			}

			return ret;
		}

		void getServiceResourceElements(RangerServiceResource serviceResource) {
			while (iterServiceResourceElement.hasNext()) {
				XXServiceResourceElement xServiceResourceElement = iterServiceResourceElement.next();

				if (xServiceResourceElement.getResourceId().equals(serviceResource.getId())) {
					RangerPolicyResource resource = new RangerPolicyResource();

					resource.setIsExcludes(xServiceResourceElement.getIsExcludes());
					resource.setIsRecursive(xServiceResourceElement.getIsRecursive());

					while (iterServiceResourceElementValue.hasNext()) {
						XXServiceResourceElementValue xServiceResourceElementValue = iterServiceResourceElementValue.next();

						if (xServiceResourceElementValue.getResElementId().equals(xServiceResourceElement.getId())) {
							resource.getValues().add(xServiceResourceElementValue.getValue());
						} else {
							if (iterServiceResourceElementValue.hasPrevious()) {
								iterServiceResourceElementValue.previous();
							}
							break;
						}
					}

					serviceResource.getResourceElements().put(lookupCache.getResourceName(xServiceResourceElement.getResDefId()), resource);
				} else if (xServiceResourceElement.getResourceId().compareTo(serviceResource.getId()) > 0) {
					if (iterServiceResourceElement.hasPrevious()) {
						iterServiceResourceElement.previous();
					}
					break;
				}
			}
		}

		boolean hasProcessedAll() {
			boolean moreToProcess = iterServiceResource.hasNext()
					|| iterServiceResourceElement.hasNext()
					|| iterServiceResourceElementValue.hasNext();
			return !moreToProcess;
		}

		List<RangerServiceResource> getServiceResourcesBySecondary() {
			List<RangerServiceResource> ret = null;

			if (service != null) {
				List<XXServiceResource> xServiceResources = filterForServicePlugin ? daoMgr.getXXServiceResource().findForServicePlugin(service.getId()) : daoMgr.getXXServiceResource().findTaggedResourcesInServiceId(service.getId());

				if (CollectionUtils.isNotEmpty(xServiceResources)) {
					ret = new ArrayList<RangerServiceResource>(xServiceResources.size());

					for (XXServiceResource xServiceResource : xServiceResources) {
						TagRetrieverServiceResourceContext ctx = new TagRetrieverServiceResourceContext(xServiceResource, service);

						RangerServiceResource serviceResource = ctx.getNextServiceResource();

						if (serviceResource != null) {
							ret.add(serviceResource);
						}
					}
				}
			}
			return ret;
		}
	}

	private class TagRetrieverTagDefContext {

		final XXService service;
		final ListIterator<XXTagDef> iterTagDef;
		final ListIterator<XXTagAttributeDef> iterTagAttributeDef;


		TagRetrieverTagDefContext(XXService xService) {
			Long serviceId = xService == null ? null : xService.getId();

			List<XXTagDef> xTagDefs = filterForServicePlugin ? daoMgr.getXXTagDef().findForServicePlugin(serviceId) : daoMgr.getXXTagDef().findByServiceId(serviceId);
			List<XXTagAttributeDef> xTagAttributeDefs = filterForServicePlugin ? daoMgr.getXXTagAttributeDef().findForServicePlugin(serviceId) : daoMgr.getXXTagAttributeDef().findByServiceId(serviceId);

			this.service = xService;
			this.iterTagDef = xTagDefs.listIterator();
			this.iterTagAttributeDef = xTagAttributeDefs.listIterator();
		}

		TagRetrieverTagDefContext(XXTagDef xTagDef, XXService xService) {
			Long tagDefId = xTagDef == null ? null : xTagDef.getId();

			List<XXTagDef> xTagDefs = asList(xTagDef);
			List<XXTagAttributeDef> xTagAttributeDefs = daoMgr.getXXTagAttributeDef().findByTagDefId(tagDefId);

			this.service = xService;
			this.iterTagDef = xTagDefs.listIterator();
			this.iterTagAttributeDef = xTagAttributeDefs.listIterator();
		}

		Map<Long, RangerTagDef> getAllTagDefs() {
			Map<Long, RangerTagDef> ret = new HashMap<Long, RangerTagDef>();

			while (iterTagDef.hasNext()) {
				RangerTagDef tagDef = getNextTagDef();

				if (tagDef != null) {
					ret.put(tagDef.getId(), tagDef);
				}
			}

			if (!hasProcessedAllTagDefs()) {
				LOG.warn("getAllTagDefs(): perhaps one or more tag-definitions got updated during retrieval.  Using fallback ... ");

				ret = getTagDefsBySecondary();

			}

			return ret;
		}

		RangerTagDef getNextTagDef() {
			RangerTagDef ret = null;

			if (iterTagDef.hasNext()) {
				XXTagDef xTagDef = iterTagDef.next();

				if (xTagDef != null) {
					ret = new RangerTagDef();

					ret.setId(xTagDef.getId());
					ret.setGuid(xTagDef.getGuid());
					ret.setIsEnabled(xTagDef.getIsEnabled());
					ret.setCreatedBy(lookupCache.getUserScreenName(xTagDef.getAddedByUserId()));
					ret.setUpdatedBy(lookupCache.getUserScreenName(xTagDef.getUpdatedByUserId()));
					ret.setCreateTime(xTagDef.getCreateTime());
					ret.setUpdateTime(xTagDef.getUpdateTime());
					ret.setVersion(xTagDef.getVersion());
					ret.setName(xTagDef.getName());
					ret.setSource(xTagDef.getSource());

					getTagAttributeDefs(ret);
				}
			}

			return ret;
		}

		void getTagAttributeDefs(RangerTagDef tagDef) {
			while (iterTagAttributeDef.hasNext()) {
				XXTagAttributeDef xTagAttributeDef = iterTagAttributeDef.next();

				if (xTagAttributeDef.getTagDefId().equals(tagDef.getId())) {
					RangerTagDef.RangerTagAttributeDef tagAttributeDef = new RangerTagDef.RangerTagAttributeDef();

					tagAttributeDef.setName(xTagAttributeDef.getName());
					tagAttributeDef.setType(xTagAttributeDef.getType());

					tagDef.getAttributeDefs().add(tagAttributeDef);
				} else if (xTagAttributeDef.getTagDefId().compareTo(tagDef.getId()) > 0) {
					if (iterTagAttributeDef.hasPrevious()) {
						iterTagAttributeDef.previous();
					}
					break;
				}
			}
		}

		boolean hasProcessedAllTagDefs() {
			boolean moreToProcess = iterTagAttributeDef.hasNext();
			return !moreToProcess;
		}

		Map<Long, RangerTagDef> getTagDefsBySecondary() {
			Map<Long, RangerTagDef> ret = null;

			if (service != null) {
				List<XXTagDef> xTagDefs = daoMgr.getXXTagDef().findByServiceId(service.getId());

				if (CollectionUtils.isNotEmpty(xTagDefs)) {
					ret = new HashMap<Long, RangerTagDef>(xTagDefs.size());

					for (XXTagDef xTagDef : xTagDefs) {
						TagRetrieverTagDefContext ctx = new TagRetrieverTagDefContext(xTagDef, service);

						RangerTagDef tagDef = ctx.getNextTagDef();

						if (tagDef != null) {
							ret.put(tagDef.getId(), tagDef);
						}
					}
				}
			}
			return ret;
		}
	}

	private class TagRetrieverTagContext {

		final XXService service;
		final ListIterator<XXTag> iterTag;
		final ListIterator<XXTagAttribute> iterTagAttribute;

		TagRetrieverTagContext(XXService xService) {
			Long serviceId = xService == null ? null : xService.getId();

			List<XXTag> xTags = filterForServicePlugin ? daoMgr.getXXTag().findForServicePlugin(serviceId) : daoMgr.getXXTag().findByServiceId(serviceId);
			List<XXTagAttribute> xTagAttributes = filterForServicePlugin ? daoMgr.getXXTagAttribute().findForServicePlugin(serviceId) : daoMgr.getXXTagAttribute().findByServiceId(serviceId);

			this.service = xService;
			this.iterTag = xTags.listIterator();
			this.iterTagAttribute = xTagAttributes.listIterator();

		}

		TagRetrieverTagContext(XXTag xTag, XXService xService) {
			Long tagId = xTag == null ? null : xTag.getId();

			List<XXTag> xTags = asList(xTag);
			List<XXTagAttribute> xTagAttributes = daoMgr.getXXTagAttribute().findByTagId(tagId);

			this.service = xService;
			this.iterTag = xTags.listIterator();
			this.iterTagAttribute = xTagAttributes.listIterator();
		}


		Map<Long, RangerTag> getAllTags() {
			Map<Long, RangerTag> ret = new HashMap<Long, RangerTag>();

			while (iterTag.hasNext()) {
				RangerTag tag = getNextTag();

				if (tag != null) {
					ret.put(tag.getId(), tag);
				}
			}

			if (!hasProcessedAllTags()) {
				LOG.warn("getAllTags(): perhaps one or more tags got updated during retrieval. Using fallback ... ");

				ret = getTagsBySecondary();
			}

			return ret;
		}

		RangerTag getNextTag() {
			RangerTag ret = null;

			if (iterTag.hasNext()) {
				XXTag xTag = iterTag.next();

				if (xTag != null) {
					ret = new RangerTag();

					ret.setId(xTag.getId());
					ret.setGuid(xTag.getGuid());
					ret.setOwner(xTag.getOwner());
					ret.setCreatedBy(lookupCache.getUserScreenName(xTag.getAddedByUserId()));
					ret.setUpdatedBy(lookupCache.getUserScreenName(xTag.getUpdatedByUserId()));
					ret.setCreateTime(xTag.getCreateTime());
					ret.setUpdateTime(xTag.getUpdateTime());
					ret.setVersion(xTag.getVersion());

					Map<Long, RangerTagDef> tagDefs = getTagDefs();
					if (tagDefs != null) {
						RangerTagDef tagDef = tagDefs.get(xTag.getType());
						if (tagDef != null) {
							ret.setType(tagDef.getName());
						}
					}

					getTagAttributes(ret);
				}
			}

			return ret;
		}

		void getTagAttributes(RangerTag tag) {
			while (iterTagAttribute.hasNext()) {
				XXTagAttribute xTagAttribute = iterTagAttribute.next();

				if (xTagAttribute.getTagId().equals(tag.getId())) {
					String attributeName = xTagAttribute.getName();
					String attributeValue = xTagAttribute.getValue();


					tag.getAttributes().put(attributeName, attributeValue);
				} else if (xTagAttribute.getTagId().compareTo(tag.getId()) > 0) {
					if (iterTagAttribute.hasPrevious()) {
						iterTagAttribute.previous();
					}
					break;
				}
			}
		}

		boolean hasProcessedAllTags() {
			boolean moreToProcess = iterTagAttribute.hasNext();
			return !moreToProcess;
		}

		Map<Long, RangerTag> getTagsBySecondary() {
			Map<Long, RangerTag> ret = null;

			if (service != null) {
				List<XXTag> xTags = daoMgr.getXXTag().findByServiceId(service.getId());

				if (CollectionUtils.isNotEmpty(xTags)) {
					ret = new HashMap<Long, RangerTag>(xTags.size());

					for (XXTag xTag : xTags) {
						TagRetrieverTagContext ctx = new TagRetrieverTagContext(xTag, service);

						RangerTag tag = ctx.getNextTag();

						if (tag != null) {
							ret.put(tag.getId(), tag);
						}
					}
				}
			}
			return ret;
		}
	}
}

