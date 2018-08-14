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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.*;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemRowFilterInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicy.RangerRowFilterPolicyItem;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyItemEvaluator;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

public class RangerPolicyRetriever {
	static final Log LOG      = LogFactory.getLog(RangerPolicyRetriever.class);
	static final Log PERF_LOG = RangerPerfTracer.getPerfLogger("db.RangerPolicyRetriever");

	private final RangerDaoManager  daoMgr;
	private final LookupCache       lookupCache = new LookupCache();

	private final PlatformTransactionManager  txManager;
	private final TransactionTemplate         txTemplate;

	public RangerPolicyRetriever(RangerDaoManager daoMgr, PlatformTransactionManager txManager) {
		this.daoMgr     = daoMgr;
		this.txManager  = txManager;
		if (this.txManager != null) {
			this.txTemplate = new TransactionTemplate(this.txManager);
			this.txTemplate.setReadOnly(true);
		} else {
			this.txTemplate = null;
		}
	}

	public RangerPolicyRetriever(RangerDaoManager daoMgr) {
		this.daoMgr      = daoMgr;
		this.txManager   = null;
		this.txTemplate  = null;
	}

	public List<RangerPolicy> getServicePolicies(Long serviceId) {
		List<RangerPolicy> ret = null;

		if(serviceId != null) {
			XXService xService = getXXService(serviceId);

			if(xService != null) {
				ret = getServicePolicies(xService);
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("RangerPolicyRetriever.getServicePolicies(serviceId=" + serviceId + "): service not found");
				}
			}
		}

		return ret;
	}

	public List<RangerPolicy> getServicePolicies(String serviceName) {
		List<RangerPolicy> ret = null;

		if(serviceName != null) {
			XXService xService = getXXService(serviceName);

			if(xService != null) {
				ret = getServicePolicies(xService);
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("RangerPolicyRetriever.getServicePolicies(serviceName=" + serviceName + "): service not found");
				}
			}
		}

		return ret;
	}

	private class PolicyLoaderThread extends Thread {
		final TransactionTemplate txTemplate;
		final XXService           xService;
		List<RangerPolicy>  policies;

		PolicyLoaderThread(TransactionTemplate txTemplate, final XXService xService) {
			this.txTemplate = txTemplate;
			this.xService   = xService;
		}

		public List<RangerPolicy> getPolicies() { return policies; }

		@Override
		public void run() {
			try {
				txTemplate.setReadOnly(true);
				policies = txTemplate.execute(new TransactionCallback<List<RangerPolicy>>() {
					@Override
					public List<RangerPolicy> doInTransaction(TransactionStatus status) {
						try {
							RetrieverContext ctx = new RetrieverContext(xService);
							return ctx.getAllPolicies();
						} catch (Exception ex) {
							LOG.error("RangerPolicyRetriever.getServicePolicies(): Failed to get policies for service:[" + xService.getName() + "] in a new transaction", ex);
							status.setRollbackOnly();
							return null;
						}
					}
				});
			} catch (Throwable ex) {
				LOG.error("RangerPolicyRetriever.getServicePolicies(): Failed to get policies for service:[" + xService.getName() + "] in a new transaction", ex);
			}
		}
	}

	public List<RangerPolicy> getServicePolicies(final XXService xService) {
		String serviceName = xService == null ? null : xService.getName();
		Long   serviceId   = xService == null ? null : xService.getId();

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyRetriever.getServicePolicies(serviceName=" + serviceName + ", serviceId=" + serviceId + ")");
		}

		List<RangerPolicy> ret  = null;
		RangerPerfTracer   perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "RangerPolicyRetriever.getServicePolicies(serviceName=" + serviceName + ",serviceId=" + serviceId + ")");
		}

		if(xService != null) {
			if (txTemplate == null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Transaction Manager is null; Retrieving policies in the existing transaction");
				}
				RetrieverContext ctx = new RetrieverContext(xService);
				ret = ctx.getAllPolicies();
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Retrieving policies in a new, read-only transaction");
				}

				PolicyLoaderThread t = new PolicyLoaderThread(txTemplate, xService);
				t.start();
				try {
					t.join();
					ret = t.getPolicies();
				} catch (InterruptedException ie) {
					LOG.error("Failed to retrieve policies in a new, read-only thread.", ie);
				}
			}
		} else {
			if(LOG.isDebugEnabled()) {
				LOG.debug("RangerPolicyRetriever.getServicePolicies(xService=" + xService + "): invalid parameter");
			}
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyRetriever.getServicePolicies(serviceName=" + serviceName + ", serviceId=" + serviceId + "): policyCount=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	public RangerPolicy getPolicy(Long policyId) {
		RangerPolicy ret = null;

		if(policyId != null) {
			XXPolicy xPolicy = getXXPolicy(policyId);

			if(xPolicy != null) {
				ret = getPolicy(xPolicy);
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("RangerPolicyRetriever.getPolicy(policyId=" + policyId + "): policy not found");
				}
			}

		}

		return ret;
	}

	public RangerPolicy getPolicy(XXPolicy xPolicy) {
		RangerPolicy ret = null;

		if(xPolicy != null) {
			XXService xService = getXXService(xPolicy.getService());

			if(xService != null) {
				ret = getPolicy(xPolicy, xService);
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("RangerPolicyRetriever.getPolicy(policyId=" + xPolicy.getId() + "): service not found (serviceId=" + xPolicy.getService() + ")");
				}
			}
		}

		return ret;
	}

	public RangerPolicy getPolicy(XXPolicy xPolicy, XXService xService) {
		Long policyId = xPolicy == null ? null : xPolicy.getId();

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyRetriever.getPolicy(" + policyId + ")");
		}

		RangerPolicy     ret  = null;
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "RangerPolicyRetriever.getPolicy(policyId=" + policyId + ")");
		}

		if(xPolicy != null && xService != null) {
			RetrieverContext ctx = new RetrieverContext(xPolicy, xService);

			ret = ctx.getNextPolicy();
		} else {
			if(LOG.isDebugEnabled()) {
				LOG.debug("RangerPolicyRetriever.getPolicy(xPolicy=" + xPolicy + ", xService=" + xService + "): invalid parameter(s)");
			}
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyRetriever.getPolicy(" + policyId + "): " + ret);
		}

		return ret;
	}

	private XXService getXXService(Long serviceId) {
		XXService ret = null;

		if(serviceId != null) {
			ret = daoMgr.getXXService().getById(serviceId);
		}

		return ret;
	}

	private XXService getXXService(String serviceName) {
		XXService ret = null;

		if(serviceName != null) {
			ret = daoMgr.getXXService().findByName(serviceName);
		}

		return ret;
	}

	private XXPolicy getXXPolicy(Long policyId) {
		XXPolicy ret = null;

		if(policyId != null) {
			ret = daoMgr.getXXPolicy().getById(policyId);
		}

		return ret;
	}

	class LookupCache {
		final Map<Long, String> userNames       = new HashMap<Long, String>();
		final Map<Long, String> userScreenNames = new HashMap<Long, String>();
		final Map<Long, String> groupNames      = new HashMap<Long, String>();
		final Map<Long, String> accessTypes     = new HashMap<Long, String>();
		final Map<Long, String> conditions      = new HashMap<Long, String>();
		final Map<Long, String> resourceDefs    = new HashMap<Long, String>();
		final Map<Long, String> dataMasks       = new HashMap<Long, String>();

		String getUserName(Long userId) {
			String ret = null;

			if(userId != null) {
				ret = userNames.get(userId);

				if(ret == null) {
					XXUser user = daoMgr.getXXUser().getById(userId);

					if(user != null) {
						ret = user.getName(); // Name is `loginId`

						userNames.put(userId,  ret);
					}
				}
			}

			return ret;
		}

		String getUserScreenName(Long userId) {
			String ret = null;

			if(userId != null) {
				ret = userScreenNames.get(userId);

				if(ret == null) {
					XXPortalUser user = daoMgr.getXXPortalUser().getById(userId);

					if(user != null) {
						ret = user.getPublicScreenName();

						if (StringUtil.isEmpty(ret)) {
							ret = user.getFirstName();

							if(StringUtil.isEmpty(ret)) {
								ret = user.getLoginId();
							} else {
								if(!StringUtil.isEmpty(user.getLastName())) {
									ret += (" " + user.getLastName());
								}
							}
						}

						if(ret != null) {
							userScreenNames.put(userId, ret);
						}
					}
				}
			}

			return ret;
		}

		String getGroupName(Long groupId) {
			String ret = null;

			if(groupId != null) {
				ret = groupNames.get(groupId);

				if(ret == null) {
					XXGroup group = daoMgr.getXXGroup().getById(groupId);

					if(group != null) {
						ret = group.getName();

						groupNames.put(groupId,  ret);
					}
				}
			}

			return ret;
		}

		String getAccessType(Long accessTypeId) {
			String ret = null;

			if(accessTypeId != null) {
				ret = accessTypes.get(accessTypeId);

				if(ret == null) {
					XXAccessTypeDef xAccessType = daoMgr.getXXAccessTypeDef().getById(accessTypeId);

					if(xAccessType != null) {
						ret = xAccessType.getName();

						accessTypes.put(accessTypeId,  ret);
					}
				}
			}

			return ret;
		}

		String getConditionType(Long conditionDefId) {
			String ret = null;

			if(conditionDefId != null) {
				ret = conditions.get(conditionDefId);

				if(ret == null) {
					XXPolicyConditionDef xPolicyConditionDef = daoMgr.getXXPolicyConditionDef().getById(conditionDefId);

					if(xPolicyConditionDef != null) {
						ret = xPolicyConditionDef.getName();

						conditions.put(conditionDefId,  ret);
					}
				}
			}

			return ret;
		}

		String getResourceName(Long resourceDefId) {
			String ret = null;

			if(resourceDefId != null) {
				ret = resourceDefs.get(resourceDefId);

				if(ret == null) {
					XXResourceDef xResourceDef = daoMgr.getXXResourceDef().getById(resourceDefId);

					if(xResourceDef != null) {
						ret = xResourceDef.getName();

						resourceDefs.put(resourceDefId,  ret);
					}
				}
			}

			return ret;
		}

		String getDataMaskName(Long dataMaskDefId) {
			String ret = null;

			if(dataMaskDefId != null) {
				ret = dataMasks.get(dataMaskDefId);

				if(ret == null) {
					XXDataMaskTypeDef xDataMaskDef = daoMgr.getXXDataMaskTypeDef().getById(dataMaskDefId);

					if(xDataMaskDef != null) {
						ret = xDataMaskDef.getName();

						dataMasks.put(dataMaskDefId,  ret);
					}
				}
			}

			return ret;
		}
	}

	static List<XXPolicy> asList(XXPolicy policy) {
		List<XXPolicy> ret = new ArrayList<XXPolicy>();

		if(policy != null) {
			ret.add(policy);
		}

		return ret;
	}

	class RetrieverContext {
		final XXService                           service;
		final ListIterator<XXPolicy>              iterPolicy;
		final ListIterator<XXPolicyResource>      iterResources;
		final ListIterator<XXPolicyResourceMap>   iterResourceMaps;
		final ListIterator<XXPolicyItem>          iterPolicyItems;
		final ListIterator<XXPolicyItemUserPerm>  iterUserPerms;
		final ListIterator<XXPolicyItemGroupPerm> iterGroupPerms;
		final ListIterator<XXPolicyItemAccess>    iterAccesses;
		final ListIterator<XXPolicyItemCondition> iterConditions;
		final ListIterator<XXPolicyItemDataMaskInfo>  iterDataMaskInfos;
		final ListIterator<XXPolicyItemRowFilterInfo> iterRowFilterInfos;

		RetrieverContext(XXService xService) {
			Long serviceId = xService == null ? null : xService.getId();

			List<XXPolicy>              xPolicies     = daoMgr.getXXPolicy().findByServiceId(serviceId);
			List<XXPolicyResource>      xResources    = daoMgr.getXXPolicyResource().findByServiceId(serviceId);
			List<XXPolicyResourceMap>   xResourceMaps = daoMgr.getXXPolicyResourceMap().findByServiceId(serviceId);
			List<XXPolicyItem>          xPolicyItems  = daoMgr.getXXPolicyItem().findByServiceId(serviceId);
			List<XXPolicyItemUserPerm>  xUserPerms    = daoMgr.getXXPolicyItemUserPerm().findByServiceId(serviceId);
			List<XXPolicyItemGroupPerm> xGroupPerms   = daoMgr.getXXPolicyItemGroupPerm().findByServiceId(serviceId);
			List<XXPolicyItemAccess>    xAccesses     = daoMgr.getXXPolicyItemAccess().findByServiceId(serviceId);
			List<XXPolicyItemCondition> xConditions   = daoMgr.getXXPolicyItemCondition().findByServiceId(serviceId);
			List<XXPolicyItemDataMaskInfo>  xDataMaskInfos  = daoMgr.getXXPolicyItemDataMaskInfo().findByServiceId(serviceId);
			List<XXPolicyItemRowFilterInfo> xRowFilterInfos = daoMgr.getXXPolicyItemRowFilterInfo().findByServiceId(serviceId);

			this.service          = xService;
			this.iterPolicy       = xPolicies.listIterator();
			this.iterResources    = xResources.listIterator();
			this.iterResourceMaps = xResourceMaps.listIterator();
			this.iterPolicyItems  = xPolicyItems.listIterator();
			this.iterUserPerms    = xUserPerms.listIterator();
			this.iterGroupPerms   = xGroupPerms.listIterator();
			this.iterAccesses     = xAccesses.listIterator();
			this.iterConditions   = xConditions.listIterator();
			this.iterDataMaskInfos  = xDataMaskInfos.listIterator();
			this.iterRowFilterInfos = xRowFilterInfos.listIterator();
		}

		RetrieverContext(XXPolicy xPolicy) {
			this(xPolicy, getXXService(xPolicy.getService()));
		}

		RetrieverContext(XXPolicy xPolicy, XXService xService) {
			Long policyId = xPolicy == null ? null : xPolicy.getId();

			List<XXPolicy>              xPolicies     = asList(xPolicy);
			List<XXPolicyResource>      xResources    = daoMgr.getXXPolicyResource().findByPolicyId(policyId);
			List<XXPolicyResourceMap>   xResourceMaps = daoMgr.getXXPolicyResourceMap().findByPolicyId(policyId);
			List<XXPolicyItem>          xPolicyItems  = daoMgr.getXXPolicyItem().findByPolicyId(policyId);
			List<XXPolicyItemUserPerm>  xUserPerms    = daoMgr.getXXPolicyItemUserPerm().findByPolicyId(policyId);
			List<XXPolicyItemGroupPerm> xGroupPerms   = daoMgr.getXXPolicyItemGroupPerm().findByPolicyId(policyId);
			List<XXPolicyItemAccess>    xAccesses     = daoMgr.getXXPolicyItemAccess().findByPolicyId(policyId);
			List<XXPolicyItemCondition> xConditions   = daoMgr.getXXPolicyItemCondition().findByPolicyId(policyId);
			List<XXPolicyItemDataMaskInfo>  xDataMaskInfos  = daoMgr.getXXPolicyItemDataMaskInfo().findByPolicyId(policyId);
			List<XXPolicyItemRowFilterInfo> xRowFilterInfos = daoMgr.getXXPolicyItemRowFilterInfo().findByPolicyId(policyId);

			this.service          = xService;
			this.iterPolicy       = xPolicies.listIterator();
			this.iterResources    = xResources.listIterator();
			this.iterResourceMaps = xResourceMaps.listIterator();
			this.iterPolicyItems  = xPolicyItems.listIterator();
			this.iterUserPerms    = xUserPerms.listIterator();
			this.iterGroupPerms   = xGroupPerms.listIterator();
			this.iterAccesses     = xAccesses.listIterator();
			this.iterConditions   = xConditions.listIterator();
			this.iterDataMaskInfos  = xDataMaskInfos.listIterator();
			this.iterRowFilterInfos = xRowFilterInfos.listIterator();
		}

		RangerPolicy getNextPolicy() {
			RangerPolicy ret = null;

			if(iterPolicy.hasNext()) {
				XXPolicy xPolicy = iterPolicy.next();

				if(xPolicy != null) {
					ret = new RangerPolicy();

					ret.setId(xPolicy.getId());
					ret.setGuid(xPolicy.getGuid());
					ret.setIsEnabled(xPolicy.getIsEnabled());
					ret.setCreatedBy(lookupCache.getUserScreenName(xPolicy.getAddedByUserId()));
					ret.setUpdatedBy(lookupCache.getUserScreenName(xPolicy.getUpdatedByUserId()));
					ret.setCreateTime(xPolicy.getCreateTime());
					ret.setUpdateTime(xPolicy.getUpdateTime());
					ret.setVersion(xPolicy.getVersion());
					ret.setService(service == null ? null : service.getName());
					ret.setName(StringUtils.trim(xPolicy.getName()));
					ret.setPolicyType(xPolicy.getPolicyType() == null ? RangerPolicy.POLICY_TYPE_ACCESS : xPolicy.getPolicyType());
					ret.setDescription(xPolicy.getDescription());
					ret.setResourceSignature(xPolicy.getResourceSignature());
					ret.setIsAuditEnabled(xPolicy.getIsAuditEnabled());

					getResource(ret);
					getPolicyItems(ret);
				}
			}

			return ret;
		}

		List<RangerPolicy> getAllPolicies() {
			List<RangerPolicy> ret = new ArrayList<RangerPolicy>();

			while(iterPolicy.hasNext()) {
				RangerPolicy policy = getNextPolicy();

				if(policy != null) {
					ret.add(policy);
				}
			}

			if(! hasProcessedAll()) {
				LOG.warn("getAllPolicies(): perhaps one or more policies got updated during retrieval. Falling back to secondary method");

				ret = getAllPoliciesBySecondary();
			}

			return ret;
		}

		List<RangerPolicy> getAllPoliciesBySecondary() {
			List<RangerPolicy> ret = null;

			if(service != null) {
				List<XXPolicy> xPolicies = daoMgr.getXXPolicy().findByServiceId(service.getId());

				if(CollectionUtils.isNotEmpty(xPolicies)) {
					ret = new ArrayList<RangerPolicy>(xPolicies.size());

					for(XXPolicy xPolicy : xPolicies) {
						RetrieverContext ctx = new RetrieverContext(xPolicy, service);

						RangerPolicy policy = ctx.getNextPolicy();

						if(policy != null) {
							ret.add(policy);
						}
					}
				}
			}

			return ret;
		}

		private boolean hasProcessedAll() {
			boolean moreToProcess =    iterPolicy.hasNext()
									|| iterResources.hasNext()
									|| iterResourceMaps.hasNext()
									|| iterPolicyItems.hasNext()
									|| iterUserPerms.hasNext()
									|| iterGroupPerms.hasNext()
									|| iterAccesses.hasNext()
									|| iterConditions.hasNext()
									|| iterDataMaskInfos.hasNext()
									|| iterRowFilterInfos.hasNext();

			return !moreToProcess;
		}

		private void getResource(RangerPolicy policy) {
			while(iterResources.hasNext()) {
				XXPolicyResource xResource = iterResources.next();

				if(xResource.getPolicyid().equals(policy.getId())) {
					RangerPolicyResource resource = new RangerPolicyResource();

					resource.setIsExcludes(xResource.getIsexcludes());
					resource.setIsRecursive(xResource.getIsrecursive());

					while(iterResourceMaps.hasNext()) {
						XXPolicyResourceMap xResourceMap = iterResourceMaps.next();

						if(xResourceMap.getResourceid().equals(xResource.getId())) {
							resource.getValues().add(xResourceMap.getValue());
						} else {
							if(iterResourceMaps.hasPrevious()) {
								iterResourceMaps.previous();
							}
							break;
						}
					}

					policy.getResources().put(lookupCache.getResourceName(xResource.getResdefid()), resource);
				} else if(xResource.getPolicyid().compareTo(policy.getId()) > 0) {
					if(iterResources.hasPrevious()) {
						iterResources.previous();
					}
					break;
				}
			}
		}

		private void getPolicyItems(RangerPolicy policy) {
			while(iterPolicyItems.hasNext()) {
				XXPolicyItem xPolicyItem = iterPolicyItems.next();

				if(xPolicyItem.getPolicyid().equals(policy.getId())) {
					final RangerPolicyItem          policyItem;
					final RangerDataMaskPolicyItem  dataMaskPolicyItem;
					final RangerRowFilterPolicyItem rowFilterPolicyItem;

					if(xPolicyItem.getItemType() == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DATAMASK) {
						dataMaskPolicyItem  = new RangerDataMaskPolicyItem();
						rowFilterPolicyItem = null;
						policyItem          = dataMaskPolicyItem;
					} else if(xPolicyItem.getItemType() == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ROWFILTER) {
						dataMaskPolicyItem  = null;
						rowFilterPolicyItem = new RangerRowFilterPolicyItem();
						policyItem          = rowFilterPolicyItem;
					} else {
						dataMaskPolicyItem  = null;
						rowFilterPolicyItem = null;
						policyItem          = new RangerPolicyItem();
					}


					while(iterAccesses.hasNext()) {
						XXPolicyItemAccess xAccess = iterAccesses.next();

						if(xAccess.getPolicyitemid().equals(xPolicyItem.getId())) {
							policyItem.getAccesses().add(new RangerPolicyItemAccess(lookupCache.getAccessType(xAccess.getType()), xAccess.getIsallowed()));
						} else {
							if(iterAccesses.hasPrevious()) {
								iterAccesses.previous();
							}
							break;
						}
					}

					while(iterUserPerms.hasNext()) {
						XXPolicyItemUserPerm xUserPerm = iterUserPerms.next();

						if(xUserPerm.getPolicyitemid().equals(xPolicyItem.getId())) {
							String userName = lookupCache.getUserName(xUserPerm.getUserid());
							if (userName != null) {
								policyItem.getUsers().add(userName);
							}
						} else {
							if(iterUserPerms.hasPrevious()) {
								iterUserPerms.previous();
							}
							break;
						}
					}

					while(iterGroupPerms.hasNext()) {
						XXPolicyItemGroupPerm xGroupPerm = iterGroupPerms.next();

						if(xGroupPerm.getPolicyitemid().equals(xPolicyItem.getId())) {
							String groupName = lookupCache.getGroupName(xGroupPerm.getGroupid());
							if (groupName != null) {
								policyItem.getGroups().add(groupName);
							}
						} else {
							if(iterGroupPerms.hasPrevious()) {
								iterGroupPerms.previous();
							}
							break;
						}
					}

					RangerPolicyItemCondition condition         = null;
					Long                      prevConditionType = null;
					while(iterConditions.hasNext()) {
						XXPolicyItemCondition xCondition = iterConditions.next();

						if(xCondition.getPolicyitemid().equals(xPolicyItem.getId())) {
							if(! xCondition.getType().equals(prevConditionType)) {
								condition = new RangerPolicyItemCondition();
								condition.setType(lookupCache.getConditionType(xCondition.getType()));
								condition.getValues().add(xCondition.getValue());

								policyItem.getConditions().add(condition);

								prevConditionType = xCondition.getType();
							} else {
								condition.getValues().add(xCondition.getValue());
							}
						} else {
							if(iterConditions.hasPrevious()) {
								iterConditions.previous();
							}
							break;
						}
					}

					policyItem.setDelegateAdmin(xPolicyItem.getDelegateAdmin());

					if(dataMaskPolicyItem != null) {
						while (iterDataMaskInfos.hasNext()) {
							XXPolicyItemDataMaskInfo xDataMaskInfo = iterDataMaskInfos.next();

							if (xDataMaskInfo.getPolicyItemId().equals(xPolicyItem.getId())) {
								dataMaskPolicyItem.setDataMaskInfo(new RangerPolicyItemDataMaskInfo(lookupCache.getDataMaskName(xDataMaskInfo.getType()), xDataMaskInfo.getConditionExpr(), xDataMaskInfo.getValueExpr()));
							} else {
								if (iterDataMaskInfos.hasPrevious()) {
									iterDataMaskInfos.previous();
								}
								break;
							}
						}
					}

					if(rowFilterPolicyItem != null) {
						while (iterRowFilterInfos.hasNext()) {
							XXPolicyItemRowFilterInfo xRowFilterInfo = iterRowFilterInfos.next();

							if (xRowFilterInfo.getPolicyItemId().equals(xPolicyItem.getId())) {
								rowFilterPolicyItem.setRowFilterInfo(new RangerPolicyItemRowFilterInfo(xRowFilterInfo.getFilterExpr()));
							} else {
								if (iterRowFilterInfos.hasPrevious()) {
									iterRowFilterInfos.previous();
								}
								break;
							}
						}
					}


					int itemType = xPolicyItem.getItemType() == null ? RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ALLOW : xPolicyItem.getItemType();

					if(itemType == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ALLOW) {
						policy.getPolicyItems().add(policyItem);
					} else if(itemType == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DENY) {
						policy.getDenyPolicyItems().add(policyItem);
					} else if(itemType == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ALLOW_EXCEPTIONS) {
						policy.getAllowExceptions().add(policyItem);
					} else if(itemType == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DENY_EXCEPTIONS) {
						policy.getDenyExceptions().add(policyItem);
					} else if(itemType == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DATAMASK) {
						policy.getDataMaskPolicyItems().add(dataMaskPolicyItem);
					} else if(itemType == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ROWFILTER) {
						policy.getRowFilterPolicyItems().add(rowFilterPolicyItem);
					} else { // unknown itemType
						LOG.warn("RangerPolicyRetriever.getPolicy(policyId=" + policy.getId() + "): ignoring unknown policyItemType " + itemType);
					}
				} else if(xPolicyItem.getPolicyid().compareTo(policy.getId()) > 0) {
					if(iterPolicyItems.hasPrevious()) {
						iterPolicyItems.previous();
					}
					break;
				}
			}
		}
	}
}

