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

package org.apache.ranger.plugin.service;

import java.util.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.audit.provider.AuditHandler;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.provider.StandAloneAuditProviderFactory;
import org.apache.ranger.authorization.hadoop.config.RangerAuditConfig;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.conditionevaluator.RangerScriptExecutionContext;
import org.apache.ranger.plugin.contextenricher.RangerContextEnricher;
import org.apache.ranger.plugin.contextenricher.RangerTagEnricher;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.policyengine.RangerResourceAccessInfo;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.*;


public class RangerBasePlugin {
	private static final Log LOG = LogFactory.getLog(RangerBasePlugin.class);

	private final RangerPluginConfig          pluginConfig;
	private final RangerPluginContext         pluginContext;
	private final Map<String, LogHistory>     logHistoryList = new Hashtable<>();
	private final int                         logInterval    = 30000; // 30 seconds
	private final DownloadTrigger             accessTrigger  = new DownloadTrigger();
	private       PolicyRefresher             refresher;
	private       RangerPolicyEngine          policyEngine;
	private       RangerAuthContext           currentAuthContext;
	private       RangerAccessResultProcessor resultProcessor;
	private       RangerRoles                 roles;
	private final List<RangerChainedPlugin>   chainedPlugins;


	public RangerBasePlugin(String serviceType, String appId) {
		this(new RangerPluginConfig(serviceType, null, appId, null, null, null));
	}

	public RangerBasePlugin(String serviceType, String serviceName, String appId) {
		this(new RangerPluginConfig(serviceType, serviceName, appId, null, null, null));
	}

	public RangerBasePlugin(RangerPluginConfig pluginConfig) {
		this.pluginConfig  = pluginConfig;
		this.pluginContext = new RangerPluginContext(pluginConfig);

		Set<String> superUsers         = toSet(pluginConfig.get(pluginConfig.getPropertyPrefix() + ".super.users"));
		Set<String> superGroups        = toSet(pluginConfig.get(pluginConfig.getPropertyPrefix() + ".super.groups"));
		Set<String> auditExcludeUsers  = toSet(pluginConfig.get(pluginConfig.getPropertyPrefix() + ".audit.exclude.users"));
		Set<String> auditExcludeGroups = toSet(pluginConfig.get(pluginConfig.getPropertyPrefix() + ".audit.exclude.groups"));
		Set<String> auditExcludeRoles  = toSet(pluginConfig.get(pluginConfig.getPropertyPrefix() + ".audit.exclude.roles"));

		setSuperUsersAndGroups(superUsers, superGroups);
		setAuditExcludedUsersGroupsRoles(auditExcludeUsers, auditExcludeGroups, auditExcludeRoles);

		RangerScriptExecutionContext.init(pluginConfig);

		this.chainedPlugins = initChainedPlugins();
	}

	public static AuditHandler getAuditProvider(String serviceName) {
		AuditProviderFactory providerFactory = RangerBasePlugin.getAuditProviderFactory(serviceName);
		AuditHandler         ret             = providerFactory.getAuditProvider();

		return ret;
	}

	public String getServiceType() {
		return pluginConfig.getServiceType();
	}

	public String getAppId() {
		return pluginConfig.getAppId();
	}

	public RangerPluginConfig getConfig() {
		return pluginConfig;
	}

	public String getClusterName() {
		return pluginConfig.getClusterName();
	}

	public RangerAuthContext getCurrentRangerAuthContext() { return currentAuthContext; }

	// For backward compatibility
	public RangerAuthContext createRangerAuthContext() { return currentAuthContext; }

	public RangerRoles getRoles() {
		return this.roles;
	}

	public void setRoles(RangerRoles roles) {
		this.roles = roles;

		RangerPolicyEngine policyEngine = this.policyEngine;

		if (policyEngine != null) {
			policyEngine.setRoles(roles);
		}

		pluginContext.notifyAuthContextChanged();
	}

	public void setAuditExcludedUsersGroupsRoles(Set<String> users, Set<String> groups, Set<String> roles) {
		pluginConfig.setAuditExcludedUsersGroupsRoles(users, groups, roles);
	}

	public void setSuperUsersAndGroups(Set<String> users, Set<String> groups) {
		pluginConfig.setSuperUsersGroups(users, groups);
	}

	public RangerServiceDef getServiceDef() {
		RangerPolicyEngine policyEngine = this.policyEngine;

		return policyEngine != null ? policyEngine.getServiceDef() : null;
	}

	public int getServiceDefId() {
		RangerServiceDef serviceDef = getServiceDef();

		return serviceDef != null && serviceDef.getId() != null ? serviceDef.getId().intValue() : -1;
	}

	public String getServiceName() {
		return pluginConfig.getServiceName();
	}

	public AuditProviderFactory getAuditProviderFactory() { return RangerBasePlugin.getAuditProviderFactory(getServiceName()); }

	public void init() {
		cleanup();

		AuditProviderFactory providerFactory = AuditProviderFactory.getInstance();

		if (!providerFactory.isInitDone()) {
			if (pluginConfig.getProperties() != null) {
				providerFactory.init(pluginConfig.getProperties(), getAppId());
			} else {
				LOG.error("Audit subsystem is not initialized correctly. Please check audit configuration. ");
				LOG.error("No authorization audits will be generated. ");
			}
		}

		refresher = new PolicyRefresher(this);
		LOG.info("Created PolicyRefresher Thread(" + refresher.getName() + ")");
		refresher.setDaemon(true);
		refresher.startRefresher();

		for (RangerChainedPlugin chainedPlugin : chainedPlugins) {
			chainedPlugin.init();
		}
	}

	public void setPolicies(ServicePolicies policies) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> setPolicies(" + policies + ")");
		}

		// guard against catastrophic failure during policy engine Initialization or
		try {
			RangerPolicyEngine oldPolicyEngine = this.policyEngine;
			ServicePolicies    servicePolicies = null;
			boolean            isNewEngineNeeded = true;
			boolean            usePolicyDeltas = false;

			if (policies == null) {
				policies = getDefaultSvcPolicies();

				if (policies == null) {
					LOG.error("Could not get default Service Policies. Keeping old policy-engine!");
					isNewEngineNeeded = false;
				}
			} else {
				Boolean hasPolicyDeltas = RangerPolicyDeltaUtil.hasPolicyDeltas(policies);

				if (hasPolicyDeltas == null) {
					LOG.warn("Downloaded policies are internally inconsistent!! [" + policies + "]. Please check server-side code! Keeping old policy-engine!");
					isNewEngineNeeded = false;
				} else {
					if (hasPolicyDeltas.equals(Boolean.TRUE)) {
						// Rebuild policies from deltas
						RangerPolicyEngineImpl policyEngine = (RangerPolicyEngineImpl) oldPolicyEngine;

						servicePolicies = ServicePolicies.applyDelta(policies, policyEngine);

						if (servicePolicies != null) {
							usePolicyDeltas = true;
						} else {
							LOG.error("Could not apply deltas=" + Arrays.toString(policies.getPolicyDeltas().toArray()));
							LOG.warn("Keeping old policy-engine!");
							isNewEngineNeeded = false;
						}
					} else {
						usePolicyDeltas = false;
					}
				}
			}

			if (isNewEngineNeeded) {
				RangerPolicyEngine newPolicyEngine      = null;
				boolean            isPolicyEngineShared = false;

				if (!usePolicyDeltas) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Creating engine from policies");
					}

					newPolicyEngine = new RangerPolicyEngineImpl(policies, pluginContext, roles);
				} else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("policy-deltas are not null");
					}

					if (CollectionUtils.isNotEmpty(policies.getPolicyDeltas()) || MapUtils.isNotEmpty(policies.getSecurityZones())) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Non empty policy-deltas found. Cloning engine using policy-deltas");
						}

						if (oldPolicyEngine != null) {
							RangerPolicyEngineImpl oldPolicyEngineImpl = (RangerPolicyEngineImpl) oldPolicyEngine;

							newPolicyEngine = RangerPolicyEngineImpl.getPolicyEngine(oldPolicyEngineImpl, policies);
						}

						if (newPolicyEngine != null) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Applied policyDeltas=" + Arrays.toString(policies.getPolicyDeltas().toArray()) + ")");
							}

							isPolicyEngineShared = true;
						} else {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Failed to apply policyDeltas=" + Arrays.toString(policies.getPolicyDeltas().toArray()) + "), Creating engine from policies");
								LOG.debug("Creating new engine from servicePolicies:[" + servicePolicies + "]");
							}

							newPolicyEngine = new RangerPolicyEngineImpl(servicePolicies, pluginContext, roles);
						}
					} else {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Empty policy-deltas. No need to change policy engine");
						}
					}
				}

				if (newPolicyEngine != null) {
					if (!isPolicyEngineShared) {
						newPolicyEngine.setUseForwardedIPAddress(pluginConfig.isUseForwardedIPAddress());
						newPolicyEngine.setTrustedProxyAddresses(pluginConfig.getTrustedProxyAddresses());
					}

					this.policyEngine       = newPolicyEngine;
					this.currentAuthContext = pluginContext.getAuthContext();

					pluginContext.notifyAuthContextChanged();

					if (oldPolicyEngine != null) {
						((RangerPolicyEngineImpl) oldPolicyEngine).releaseResources(!isPolicyEngineShared);
					}

					if (this.refresher != null) {
						this.refresher.saveToCache(usePolicyDeltas ? servicePolicies : policies);
					}
				}

			} else {
				LOG.warn("Returning without saving policies to cache. Leaving current policy engine as-is");
			}

		} catch (Exception e) {
			LOG.error("setPolicies: policy engine initialization failed!  Leaving current policy engine as-is. Exception : ", e);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== setPolicies(" + policies + ")");
		}
	}

	public void cleanup() {
		PolicyRefresher refresher = this.refresher;
		this.refresher    = null;

		RangerPolicyEngine policyEngine = this.policyEngine;
		this.policyEngine    = null;

		if (refresher != null) {
			refresher.stopRefresher();
		}

		if (policyEngine != null) {
			((RangerPolicyEngineImpl) policyEngine).releaseResources(true);
		}
	}

	public void setResultProcessor(RangerAccessResultProcessor resultProcessor) {
		this.resultProcessor = resultProcessor;
	}

	public RangerAccessResultProcessor getResultProcessor() {
		return this.resultProcessor;
	}

	public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
		return isAccessAllowed(request, resultProcessor);
	}

	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests) {
		return isAccessAllowed(requests, resultProcessor);
	}

	public RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		RangerAccessResult ret          = null;
		RangerPolicyEngine policyEngine = this.policyEngine;

		if (policyEngine != null) {
			ret = policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ACCESS, null);
		}

		if (ret != null) {
			for (RangerChainedPlugin chainedPlugin : chainedPlugins) {
				RangerAccessResult chainedResult = chainedPlugin.isAccessAllowed(request);

				if (chainedResult != null) {
					updateResultFromChainedResult(ret, chainedResult);
				}
			}

		}

		if (resultProcessor != null) {
			resultProcessor.processResult(ret);
		}

		return ret;
	}

	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAccessResultProcessor resultProcessor) {
		Collection<RangerAccessResult> ret          = null;
		RangerPolicyEngine             policyEngine = this.policyEngine;

		if (policyEngine != null) {
			ret = policyEngine.evaluatePolicies(requests, RangerPolicy.POLICY_TYPE_ACCESS, null);
		}

		if (CollectionUtils.isNotEmpty(ret)) {
			for (RangerChainedPlugin chainedPlugin : chainedPlugins) {
				Collection<RangerAccessResult> chainedResults = chainedPlugin.isAccessAllowed(requests);

				if (CollectionUtils.isNotEmpty(chainedResults)) {
					Iterator<RangerAccessResult> iterRet            = ret.iterator();
					Iterator<RangerAccessResult> iterChainedResults = chainedResults.iterator();

					while (iterRet.hasNext() && iterChainedResults.hasNext()) {
						RangerAccessResult result        = iterRet.next();
						RangerAccessResult chainedResult = iterChainedResults.next();

						if (result != null && chainedResult != null) {
							updateResultFromChainedResult(result, chainedResult);
						}
					}
				}
			}
		}

		if (resultProcessor != null) {
			resultProcessor.processResults(ret);
		}

		return ret;
	}

	public RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			return policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_DATAMASK, resultProcessor);
		}

		return null;
	}

	public RangerAccessResult evalRowFilterPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			return policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ROWFILTER, resultProcessor);
		}

		return null;
	}

	public RangerResourceAccessInfo getResourceAccessInfo(RangerAccessRequest request) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			return policyEngine.getResourceAccessInfo(request);
		}

		return null;
	}

	public RangerResourceACLs getResourceACLs(RangerAccessRequest request) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			return policyEngine.getResourceACLs(request);
		}

		return null;
	}

	public Set<String> getRolesFromUserAndGroups(String user, Set<String> groups) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			return policyEngine.getRolesFromUserAndGroups(user, groups);
		}

		return null;
	}

	public RangerRole createRole(RangerRole request, RangerAccessResultProcessor resultProcessor) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBasePlugin.createRole(" + request + ")");
		}

		RangerRole ret = getAdminClient().createRole(request);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBasePlugin.createRole(" + request + ")");
		}
		return ret;
	}

	public void dropRole(String execUser, String roleName, RangerAccessResultProcessor resultProcessor) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBasePlugin.dropRole(" + roleName + ")");
		}

		getAdminClient().dropRole(execUser, roleName);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBasePlugin.dropRole(" + roleName + ")");
		}
	}

	public List<String> getUserRoles(String execUser, RangerAccessResultProcessor resultProcessor) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBasePlugin.getUserRoleNames(" + execUser + ")");
		}

		final List<String> ret = getAdminClient().getUserRoles(execUser);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBasePlugin.getUserRoleNames(" + execUser + ")");
		}
		return ret;
	}

	public List<String> getAllRoles(String execUser, RangerAccessResultProcessor resultProcessor) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBasePlugin.getAllRoles()");
		}

		final List<String> ret = getAdminClient().getAllRoles(execUser);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBasePlugin.getAllRoles()");
		}
		return ret;
	}

	public RangerRole getRole(String execUser, String roleName, RangerAccessResultProcessor resultProcessor) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBasePlugin.getPrincipalsForRole(" + roleName + ")");
		}

		final RangerRole ret = getAdminClient().getRole(execUser, roleName);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBasePlugin.getPrincipalsForRole(" + roleName + ")");
		}
		return ret;
	}

	public void grantRole(GrantRevokeRoleRequest request, RangerAccessResultProcessor resultProcessor) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBasePlugin.grantRole(" + request + ")");
		}

		getAdminClient().grantRole(request);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBasePlugin.grantRole(" + request + ")");
		}
	}

	public void revokeRole(GrantRevokeRoleRequest request, RangerAccessResultProcessor resultProcessor) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBasePlugin.revokeRole(" + request + ")");
		}

		getAdminClient().revokeRole(request);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBasePlugin.revokeRole(" + request + ")");
		}
	}

	public void grantAccess(GrantRevokeRequest request, RangerAccessResultProcessor resultProcessor) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBasePlugin.grantAccess(" + request + ")");
		}

		boolean isSuccess = false;

		try {
			RangerPolicyEngine policyEngine = this.policyEngine;

			if (policyEngine != null) {
				request.setZoneName(policyEngine.getUniquelyMatchedZoneName(request));
			}

			getAdminClient().grantAccess(request);

			isSuccess = true;
		} finally {
			auditGrantRevoke(request, "grant", isSuccess, resultProcessor);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBasePlugin.grantAccess(" + request + ")");
		}
	}

	public void revokeAccess(GrantRevokeRequest request, RangerAccessResultProcessor resultProcessor) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBasePlugin.revokeAccess(" + request + ")");
		}

		boolean isSuccess = false;

		try {
			RangerPolicyEngine policyEngine = this.policyEngine;

			if (policyEngine != null) {
				request.setZoneName(policyEngine.getUniquelyMatchedZoneName(request));
			}

			getAdminClient().revokeAccess(request);

			isSuccess = true;
		} finally {
			auditGrantRevoke(request, "revoke", isSuccess, resultProcessor);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBasePlugin.revokeAccess(" + request + ")");
		}
	}

	public void registerAuthContextEventListener(RangerAuthContextListener authContextListener) {
		this.pluginContext.setAuthContextListener(authContextListener);
	}

	public static RangerAdminClient createAdminClient(RangerPluginConfig pluginConfig) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBasePlugin.createAdminClient(" + pluginConfig.getServiceName() + ", " + pluginConfig.getAppId() + ", " + pluginConfig.getPropertyPrefix() + ")");
		}

		RangerAdminClient ret              = null;
		String            propertyName     = pluginConfig.getPropertyPrefix() + ".policy.source.impl";
		String            policySourceImpl = pluginConfig.get(propertyName);

		if(StringUtils.isEmpty(policySourceImpl)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Value for property[%s] was null or empty. Unexpected! Will use policy source of type[%s]", propertyName, RangerAdminRESTClient.class.getName()));
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Value for property[%s] was [%s].", propertyName, policySourceImpl));
			}

			try {
				@SuppressWarnings("unchecked")
				Class<RangerAdminClient> adminClass = (Class<RangerAdminClient>)Class.forName(policySourceImpl);
				
				ret = adminClass.newInstance();
			} catch (Exception excp) {
				LOG.error("failed to instantiate policy source of type '" + policySourceImpl + "'. Will use policy source of type '" + RangerAdminRESTClient.class.getName() + "'", excp);
			}
		}

		if(ret == null) {
			ret = new RangerAdminRESTClient();
		}

		ret.init(pluginConfig.getServiceName(), pluginConfig.getAppId(), pluginConfig.getPropertyPrefix(), pluginConfig);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBasePlugin.createAdminClient(" + pluginConfig.getServiceName() + ", " + pluginConfig.getAppId() + ", " + pluginConfig.getPropertyPrefix() + "): policySourceImpl=" + policySourceImpl + ", client=" + ret);
		}
		return ret;
	}

	public void refreshPoliciesAndTags() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> refreshPoliciesAndTags()");
		}

		try {
			RangerPolicyEngine policyEngine = this.policyEngine;

			// Synch-up policies
			long oldPolicyVersion = policyEngine.getPolicyVersion();

			refresher.syncPoliciesWithAdmin(accessTrigger);

			policyEngine = this.policyEngine; // might be updated in syncPoliciesWithAdmin()

			long newPolicyVersion = policyEngine.getPolicyVersion();

			if (oldPolicyVersion == newPolicyVersion) {
				// Synch-up tags
				RangerTagEnricher tagEnricher = getTagEnricher();

				if (tagEnricher != null) {
					tagEnricher.syncTagsWithAdmin(accessTrigger);
				}
			}
		} catch (InterruptedException exception) {
			LOG.error("Failed to update policy-engine, continuing to use old policy-engine and/or tags", exception);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== refreshPoliciesAndTags()");
		}
	}


	private void auditGrantRevoke(GrantRevokeRequest request, String action, boolean isSuccess, RangerAccessResultProcessor resultProcessor) {
		if(request != null && resultProcessor != null) {
			RangerAccessRequestImpl accessRequest = new RangerAccessRequestImpl();
	
			accessRequest.setResource(new RangerAccessResourceImpl(StringUtil.toStringObjectMap(request.getResource())));
			accessRequest.setUser(request.getGrantor());
			accessRequest.setAccessType(RangerPolicyEngine.ADMIN_ACCESS);
			accessRequest.setAction(action);
			accessRequest.setClientIPAddress(request.getClientIPAddress());
			accessRequest.setClientType(request.getClientType());
			accessRequest.setRequestData(request.getRequestData());
			accessRequest.setSessionId(request.getSessionId());

			// call isAccessAllowed() to determine if audit is enabled or not
			RangerAccessResult accessResult = isAccessAllowed(accessRequest, null);

			if(accessResult != null && accessResult.getIsAudited()) {
				accessRequest.setAccessType(action);
				accessResult.setIsAllowed(isSuccess);

				if(! isSuccess) {
					accessResult.setPolicyId(-1);
				}

				resultProcessor.processResult(accessResult);
			}
		}
	}

	private RangerServiceDef getDefaultServiceDef() {
		RangerServiceDef ret = null;

		if (StringUtils.isNotBlank(getServiceType())) {
			try {
				ret = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(getServiceType());
			} catch (Exception exp) {
				LOG.error("Could not get embedded service-def for " + getServiceType());
			}
		}
		return ret;
	}

	private ServicePolicies getDefaultSvcPolicies() {
		ServicePolicies  ret        = null;
		RangerServiceDef serviceDef = getServiceDef();

		if (serviceDef == null) {
			serviceDef = getDefaultServiceDef();
		}

		if (serviceDef != null) {
			ret = new ServicePolicies();

			ret.setServiceDef(serviceDef);
			ret.setServiceName(getServiceName());
			ret.setPolicies(new ArrayList<RangerPolicy>());
		}

		return ret;
	}

	public boolean logErrorMessage(String message) {
		LogHistory log = logHistoryList.get(message);
		if (log == null) {
			log = new LogHistory();
			logHistoryList.put(message, log);
		}
		if ((System.currentTimeMillis() - log.lastLogTime) > logInterval) {
			log.lastLogTime = System.currentTimeMillis();
			int counter = log.counter;
			log.counter = 0;
			if( counter > 0) {
				message += ". Messages suppressed before: " + counter;
			}
			LOG.error(message);
			return true;
		} else {
			log.counter++;
		}
		return false;
	}

	private Set<String> toSet(String value) {
		return StringUtils.isNotBlank(value) ? StringUtil.toSet(value) : Collections.emptySet();
	}

	static private final class LogHistory {
		long lastLogTime;
		int counter;
	}

	private RangerTagEnricher getTagEnricher() {
		RangerTagEnricher ret         = null;
		RangerAuthContext authContext = getCurrentRangerAuthContext();

		if (authContext != null) {
			Map<RangerContextEnricher, Object> contextEnricherMap = authContext.getRequestContextEnrichers();

			if (MapUtils.isNotEmpty(contextEnricherMap)) {
				Set<RangerContextEnricher> contextEnrichers = contextEnricherMap.keySet();

				for (RangerContextEnricher enricher : contextEnrichers) {
					if (enricher instanceof RangerTagEnricher) {
						ret = (RangerTagEnricher) enricher;

						break;
					}
				}
			}
		}
		return ret;
	}

	private RangerAdminClient getAdminClient() throws Exception {
		PolicyRefresher   refresher = this.refresher;
		RangerAdminClient admin     = refresher == null ? null : refresher.getRangerAdminClient();

		if(admin == null) {
			throw new Exception("ranger-admin client is null");
		}
		return admin;
	}

	private List<RangerChainedPlugin> initChainedPlugins() {
		List<RangerChainedPlugin> ret                      = new ArrayList<>();
		String                    chainedServicePropPrefix = pluginConfig.getPropertyPrefix() + ".chained.services";

		for (String chainedService : StringUtil.toList(pluginConfig.get(chainedServicePropPrefix))) {
			if (StringUtils.isBlank(chainedService)) {
				continue;
			}

			String className = pluginConfig.get(chainedServicePropPrefix + "." + chainedService + ".impl");

			if (StringUtils.isBlank(className)) {
				LOG.error("Ignoring chained service " + chainedService + ": no impl class specified");

				continue;
			}

			try {
				@SuppressWarnings("unchecked")
				Class<RangerChainedPlugin> pluginClass   = (Class<RangerChainedPlugin>) Class.forName(className);
				RangerChainedPlugin        chainedPlugin = pluginClass.getConstructor(RangerBasePlugin.class, String.class).newInstance(this, chainedService);

				ret.add(chainedPlugin);
			} catch (Throwable t) {
				LOG.error("initChainedPlugins(): error instantiating plugin impl " + className, t);
			}
		}

		return ret;
	}

	private void updateResultFromChainedResult(RangerAccessResult result, RangerAccessResult chainedResult) {
		boolean overrideResult = false;

		if (chainedResult.getIsAccessDetermined()) { // only if chained-result is definitive
			// override if result is not definitive or chained-result is by a higher priority policy
			overrideResult = !result.getIsAccessDetermined() || chainedResult.getPolicyPriority() > result.getPolicyPriority();

			if (!overrideResult) {
				// override if chained-result is from the same policy priority, and if denies access
				if (chainedResult.getPolicyPriority() == result.getPolicyPriority() && !chainedResult.getIsAllowed()) {
					// let's not override if result is already denied
					if (result.getIsAllowed()) {
						overrideResult = true;
					}
				}
			}
		}

		if (overrideResult) {
			result.setIsAllowed(chainedResult.getIsAllowed());
			result.setIsAccessDetermined(chainedResult.getIsAccessDetermined());
			result.setPolicyId(chainedResult.getPolicyId());
			result.setPolicyVersion(chainedResult.getPolicyVersion());
			result.setPolicyPriority(chainedResult.getPolicyPriority());
		}

		if (!result.getIsAuditedDetermined() && chainedResult.getIsAuditedDetermined()) {
			result.setIsAudited(chainedResult.getIsAudited());
			result.setAuditPolicyId(chainedResult.getAuditPolicyId());
		}
	}

	private static AuditProviderFactory getAuditProviderFactory(String serviceName) {
		AuditProviderFactory ret = AuditProviderFactory.getInstance();

		if (!ret.isInitDone()) {
			LOG.warn("RangerBasePlugin.getAuditProviderFactory(serviceName=" + serviceName + "): audit not initialized yet. Will use stand-alone audit factory");

			ret = StandAloneAuditProviderFactory.getInstance();

			if (!ret.isInitDone()) {
				RangerAuditConfig conf = new RangerAuditConfig();

				if (conf.isInitSuccess()) {
					ret.init(conf.getProperties(), "StandAlone");
				}
			}
		}

		return ret;
	}
}
