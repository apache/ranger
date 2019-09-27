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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
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
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.utils.StringUtil;
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
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyengine.RangerResourceAccessInfo;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.*;


public class RangerBasePlugin {
	private static final Log LOG = LogFactory.getLog(RangerBasePlugin.class);

	public static final char RANGER_TRUSTED_PROXY_IPADDRESSES_SEPARATOR_CHAR = ',';

	private static Map<String, RangerBasePlugin> servicePluginMap = new ConcurrentHashMap<>();

	private String                    serviceType;
	private String                    appId;
	private String                    serviceName;
	private String                    clusterName;
	private PolicyRefresher           refresher;
	private RangerPolicyEngine        policyEngine;
	private RangerPolicyEngineOptions policyEngineOptions = new RangerPolicyEngineOptions();
	private RangerPluginContext       rangerPluginContext;
	private RangerAuthContext         currentAuthContext;
	private RangerAccessResultProcessor resultProcessor;
	private boolean                   useForwardedIPAddress;
	private String[]                  trustedProxyAddresses;
	private Timer                     policyDownloadTimer;
	private Timer                     policyEngineRefreshTimer;
	private RangerAuthContextListener authContextListener;
	private AuditProviderFactory      auditProviderFactory;
	private RangerRolesProvider		  rangerRolesProvider;
	private RangerRoles               rangerRoles;

	private final BlockingQueue<DownloadTrigger> policyDownloadQueue = new LinkedBlockingQueue<>();
	private final DownloadTrigger                accessTrigger       = new DownloadTrigger();


	Map<String, LogHistory> logHistoryList = new Hashtable<String, RangerBasePlugin.LogHistory>();
	int                     logInterval    = 30000; // 30 seconds

	public static Map<String, RangerBasePlugin> getServicePluginMap() {
		return servicePluginMap;
	}

	public static AuditHandler getAuditProvider(String serviceName) {
		AuditHandler ret = null;

		boolean useStandaloneAuditProvider = false;

		if (StringUtils.isNotEmpty(serviceName)) {
			RangerBasePlugin plugin = RangerBasePlugin.getServicePluginMap().get(serviceName);
			if (plugin != null) {
				if (plugin.getAuditProviderFactory() != null) {
					ret = plugin.getAuditProviderFactory().getAuditProvider();
				} else {
					LOG.error("NULL AuditProviderFactory for serviceName:[" + serviceName + "]");
				}
			} else {
				useStandaloneAuditProvider = true;
			}
		} else {
			useStandaloneAuditProvider = true;
		}

		if (useStandaloneAuditProvider) {
			StandAloneAuditProviderFactory factory = StandAloneAuditProviderFactory.getInstance();
			if (factory.isInitDone()) {
				ret = factory.getAuditProvider();
			} else {
				RangerConfiguration conf = RangerConfiguration.getInstance();
				String auditCfg = "ranger-standalone-audit.xml";
				if (conf.addResourceIfReadable(auditCfg)) {
					factory.init(conf.getProperties(), "StandAlone");
					ret = factory.getAuditProvider();
				} else {
					LOG.error("StandAlone audit handler configuration not readable:[" + auditCfg + "]");
				}
			}
		}

		return ret;
	}

	public RangerBasePlugin(String serviceType, String appId) {
		this.serviceType = serviceType;
		this.appId       = appId;
	}

	public String getServiceType() {
		return serviceType;
	}

	public String getClusterName() {
		return clusterName;
	}

	public RangerAuthContext createRangerAuthContext() {
		return new RangerAuthContext(currentAuthContext);
	}

	public RangerAuthContext getCurrentRangerAuthContext() { return currentAuthContext; }

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public RangerRoles getRangerRoles() {
		return this.rangerRoles;
	}

	public void setRangerRoles(RangerRoles rangerRoles) {
		this.rangerRoles = rangerRoles;
	}

	public RangerServiceDef getServiceDef() {
		RangerPolicyEngine policyEngine = this.policyEngine;

		return policyEngine != null ? policyEngine.getServiceDef() : null;
	}

	public int getServiceDefId() {
		RangerServiceDef serviceDef = getServiceDef();

		return serviceDef != null && serviceDef.getId() != null ? serviceDef.getId().intValue() : -1;
	}

	public String getAppId() {
		return appId;
	}

	public String getServiceName() {
		return serviceName;
	}

	public AuditProviderFactory getAuditProviderFactory() { return auditProviderFactory; }

	public void init() {
		cleanup();

		RangerConfiguration configuration = RangerConfiguration.getInstance();
		configuration.addResourcesForServiceType(serviceType);

		String propertyPrefix    = "ranger.plugin." + serviceType;
		long   pollingIntervalMs = configuration.getLong(propertyPrefix + ".policy.pollIntervalMs", 30 * 1000);
		String cacheDir          = configuration.get(propertyPrefix + ".policy.cache.dir");
		serviceName = configuration.get(propertyPrefix + ".service.name");
		clusterName = RangerConfiguration.getInstance().get(propertyPrefix + ".access.cluster.name", "");
		if(StringUtil.isEmpty(clusterName)){
			clusterName = RangerConfiguration.getInstance().get(propertyPrefix + ".ambari.cluster.name", "");
		}
		useForwardedIPAddress = configuration.getBoolean(propertyPrefix + ".use.x-forwarded-for.ipaddress", false);
		String trustedProxyAddressString = configuration.get(propertyPrefix + ".trusted.proxy.ipaddresses");
		trustedProxyAddresses = StringUtils.split(trustedProxyAddressString, RANGER_TRUSTED_PROXY_IPADDRESSES_SEPARATOR_CHAR);
		if (trustedProxyAddresses != null) {
			for (int i = 0; i < trustedProxyAddresses.length; i++) {
				trustedProxyAddresses[i] = trustedProxyAddresses[i].trim();
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug(propertyPrefix + ".use.x-forwarded-for.ipaddress:" + useForwardedIPAddress);
			LOG.debug(propertyPrefix + ".trusted.proxy.ipaddresses:[" + StringUtils.join(trustedProxyAddresses, ", ") + "]");
		}

		if (useForwardedIPAddress && StringUtils.isBlank(trustedProxyAddressString)) {
			LOG.warn("Property " + propertyPrefix + ".use.x-forwarded-for.ipaddress" + " is set to true, and Property "
					+ propertyPrefix + ".trusted.proxy.ipaddresses" + " is not set");
			LOG.warn("Ranger plugin will trust RemoteIPAddress and treat first X-Forwarded-Address in the access-request as the clientIPAddress");
		}

		if (configuration.getProperties() != null) {
			auditProviderFactory = new AuditProviderFactory();
			auditProviderFactory.init(configuration.getProperties(), appId);
		} else {
			LOG.error("Audit subsystem is not initialized correctly. Please check audit configuration. ");
			LOG.error("No authorization audits will be generated. ");
			auditProviderFactory = null;
		}

		rangerPluginContext = new RangerPluginContext(serviceType);

		policyEngineOptions.configureForPlugin(configuration, propertyPrefix);

		LOG.info(policyEngineOptions);

		servicePluginMap.put(serviceName, this);

		RangerAdminClient admin = createAdminClient(serviceName, appId, propertyPrefix);

		rangerRolesProvider = new RangerRolesProvider(serviceType, appId, serviceName, admin,  cacheDir);

		refresher = new PolicyRefresher(this, serviceType, appId, serviceName, admin, policyDownloadQueue, cacheDir, rangerRolesProvider);
		refresher.setDaemon(true);
		refresher.startRefresher();

		policyDownloadTimer = new Timer("policyDownloadTimer", true);

		try {
			policyDownloadTimer.schedule(new DownloaderTask(policyDownloadQueue), pollingIntervalMs, pollingIntervalMs);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Scheduled policyDownloadRefresher to download policies every " + pollingIntervalMs + " milliseconds");
			}
		} catch (IllegalStateException exception) {
			LOG.error("Error scheduling policyDownloadTimer:", exception);
			LOG.error("*** Policies will NOT be downloaded every " + pollingIntervalMs + " milliseconds ***");
			policyDownloadTimer = null;
		}

		long policyReorderIntervalMs = configuration.getLong(propertyPrefix + ".policy.policyReorderInterval", 60 * 1000);
		if (policyReorderIntervalMs >= 0 && policyReorderIntervalMs < 15 * 1000) {
			policyReorderIntervalMs = 15 * 1000;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug(propertyPrefix + ".policy.policyReorderInterval:" + policyReorderIntervalMs);
		}

		if (policyEngineOptions.disableTrieLookupPrefilter && policyReorderIntervalMs > 0) {
			policyEngineRefreshTimer = new Timer("PolicyEngineRefreshTimer", true);
			try {
				policyEngineRefreshTimer.schedule(new PolicyEngineRefresher(this), policyReorderIntervalMs, policyReorderIntervalMs);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Scheduled PolicyEngineRefresher to reorder policies based on number of evaluations in and every " + policyReorderIntervalMs + " milliseconds");
				}
			} catch (IllegalStateException exception) {
				LOG.error("Error scheduling policyEngineRefresher:", exception);
				LOG.error("*** PolicyEngine will NOT be reorderd based on number of evaluations every " + policyReorderIntervalMs + " milliseconds ***");
				policyEngineRefreshTimer = null;
			}
		} else {
			LOG.info("Policies will NOT be reordered based on number of evaluations");
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
			boolean            isValid         = true;
			boolean            usePolicyDeltas = false;
			boolean            updateRangerRolesOnly = false;

			if (policies == null) {
				policies = getDefaultSvcPolicies();
				if (policies == null) {
					LOG.error("Could not get default Service Policies");
					isValid = false;
				}
			} else {
				if ((policies.getPolicies() != null && policies.getPolicyDeltas() != null)) {
					LOG.error("Invalid servicePolicies: Both policies and policy-deltas cannot be null OR both of them cannot be non-null");
					isValid = false;
				} else if (policies.getPolicies() != null) {
					usePolicyDeltas = false;
				} else if (policies.getPolicyDeltas() != null) {
					// Rebuild policies from deltas
					RangerPolicyEngineImpl policyEngine = (RangerPolicyEngineImpl) oldPolicyEngine;
					servicePolicies = ServicePolicies.applyDelta(policies, policyEngine);
					if (servicePolicies != null) {
						usePolicyDeltas = true;
					} else {
						isValid = false;
						LOG.error("Could not apply deltas=" + Arrays.toString(policies.getPolicyDeltas().toArray()));
					}
				} else if (policies.getPolicies() == null && policies.getPolicyDeltas() == null && rangerRoles != null) {
					// When no policies changes and only the role changes happens then update the policyengine with Role changes only.
					updateRangerRolesOnly = true;
				} else {
					LOG.error("Should not get here!!");
					isValid = false;
				}
			}

			if (isValid) {
				RangerPolicyEngine newPolicyEngine = null;

				if(updateRangerRolesOnly) {
					this.policyEngine.setRangerRoles(rangerRoles);
				} else if (!usePolicyDeltas) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("policies are not null. Creating engine from policies");
					}
					newPolicyEngine = new RangerPolicyEngineImpl(appId, policies, policyEngineOptions, rangerPluginContext, rangerRoles);
				} else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("policy-deltas are not null");
					}
					if (CollectionUtils.isNotEmpty(policies.getPolicyDeltas()) || MapUtils.isNotEmpty(policies.getSecurityZones())) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Non empty policy-deltas found. Cloning engine using policy-deltas");
						}
						newPolicyEngine = oldPolicyEngine.cloneWithDelta(policies, rangerRoles);
						if (newPolicyEngine != null) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Applied policyDeltas=" + Arrays.toString(policies.getPolicyDeltas().toArray()) + ")");
							}
						} else {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Failed to apply policyDeltas=" + Arrays.toString(policies.getPolicyDeltas().toArray()) + "), Creating engine from policies");
								LOG.debug("Creating new engine from servicePolicies:[" + servicePolicies + "]");
							}
							newPolicyEngine = new RangerPolicyEngineImpl(appId, servicePolicies, policyEngineOptions, rangerPluginContext, rangerRoles);
						}
					} else {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Empty policy-deltas. No need to change policy engine");
						}
					}
				}

				if (newPolicyEngine != null) {

					newPolicyEngine.setUseForwardedIPAddress(useForwardedIPAddress);
					newPolicyEngine.setTrustedProxyAddresses(trustedProxyAddresses);
					this.policyEngine = newPolicyEngine;
					this.currentAuthContext = new RangerAuthContext(rangerPluginContext.getAuthContext());

					contextChanged();

					if (oldPolicyEngine != null && !oldPolicyEngine.preCleanup()) {
						LOG.error("preCleanup() failed on the previous policy engine instance !!");
					}
					if (this.refresher != null) {
						this.refresher.saveToCache(usePolicyDeltas ? servicePolicies : policies);
					}
				}

			} else {
				LOG.error("Returning without saving policies to cache. Leaving current policy engine as-is");
			}

		} catch (Exception e) {
			LOG.error("setPolicies: policy engine initialization failed!  Leaving current policy engine as-is. Exception : ", e);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== setPolicies(" + policies + ")");
		}
	}

	public void contextChanged() {
		RangerAuthContextListener authContextListener = this.authContextListener;

		if (authContextListener != null) {
			authContextListener.contextChanged();
		}
	}

	public void cleanup() {

		PolicyRefresher refresher = this.refresher;

		RangerPolicyEngine policyEngine = this.policyEngine;

		Timer policyEngineRefreshTimer = this.policyEngineRefreshTimer;

		Timer policyDownloadTimer = this.policyDownloadTimer;

		String serviceName = this.serviceName;

		this.serviceName  = null;
		this.policyEngine = null;
		this.refresher    = null;
		this.policyEngineRefreshTimer = null;
		this.policyDownloadTimer = null;

		if (refresher != null) {
			refresher.stopRefresher();
		}

		if (policyDownloadTimer != null) {
			policyDownloadTimer.cancel();
		}

		if (policyEngineRefreshTimer != null) {
			policyEngineRefreshTimer.cancel();
		}

		if (policyEngine != null) {
			policyEngine.cleanup();
		}

		if (serviceName != null) {
			servicePluginMap.remove(serviceName);
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
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			policyEngine.preProcess(request);

			return policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ACCESS, resultProcessor);
		}

		return null;
	}

	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAccessResultProcessor resultProcessor) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			policyEngine.preProcess(requests);

			return policyEngine.evaluatePolicies(requests, RangerPolicy.POLICY_TYPE_ACCESS, resultProcessor);
		}

		return null;
	}

	public RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			policyEngine.preProcess(request);

			return policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_DATAMASK, resultProcessor);
		}

		return null;
	}

	public RangerAccessResult evalRowFilterPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			policyEngine.preProcess(request);

			return policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ROWFILTER, resultProcessor);
		}

		return null;
	}

	public RangerResourceAccessInfo getResourceAccessInfo(RangerAccessRequest request) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			policyEngine.preProcess(request);

			return policyEngine.getResourceAccessInfo(request);
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
		boolean           isSuccess = false;

		try {
			if (policyEngine != null) {
				request.setZoneName(policyEngine.getMatchedZoneName(request));
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
		boolean           isSuccess = false;

		try {
			if (policyEngine != null) {
				request.setZoneName(policyEngine.getMatchedZoneName(request));
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
		this.authContextListener = authContextListener;
	}

	public void unregisterAuthContextEventListener(RangerAuthContextListener authContextListener) {
		this.authContextListener = null;
	}

	public static RangerAdminClient createAdminClient(String rangerServiceName, String applicationId, String propertyPrefix) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBasePlugin.createAdminClient(" + rangerServiceName + ", " + applicationId + ", " + propertyPrefix + ")");
		}

		RangerAdminClient ret = null;

		String propertyName = propertyPrefix + ".policy.source.impl";
		String policySourceImpl = RangerConfiguration.getInstance().get(propertyName);

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

		ret.init(rangerServiceName, applicationId, propertyPrefix);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBasePlugin.createAdminClient(" + rangerServiceName + ", " + applicationId + ", " + propertyPrefix + "): policySourceImpl=" + policySourceImpl + ", client=" + ret);
		}
		return ret;
	}

	public void refreshPoliciesAndTags() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> refreshPoliciesAndTags()");
		}
		try {
			// Synch-up policies
			long oldPolicyVersion = this.policyEngine.getPolicyVersion();
			syncPoliciesWithAdmin(accessTrigger);
			long newPolicyVersion = this.policyEngine.getPolicyVersion();

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
			LOG.info("<== refreshPoliciesAndTags()");
		}
	}


	/*
		This API is provided only for unit testing
	 */

	public void setPluginContext(RangerPluginContext pluginContext) {
		this.rangerPluginContext = pluginContext;
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

		if (StringUtils.isNotBlank(serviceType)) {
			try {
				ret = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(serviceType);
			} catch (Exception exp) {
				LOG.error("Could not get embedded service-def for " + serviceType);
			}
		}
		return ret;
	}

	private ServicePolicies getDefaultSvcPolicies() {
		ServicePolicies ret = null;

		RangerServiceDef serviceDef = getServiceDef();
		if (serviceDef == null) {
			serviceDef = getDefaultServiceDef();
		}
		if (serviceDef != null) {
			ret = new ServicePolicies();
			ret.setServiceDef(serviceDef);
			ret.setServiceName(serviceName);
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

	static private final class LogHistory {
		long lastLogTime;
		int counter;
	}

	static private final class PolicyEngineRefresher extends TimerTask {
		private final RangerBasePlugin plugin;

		PolicyEngineRefresher(RangerBasePlugin plugin) {
			this.plugin = plugin;
		}

		@Override
		public void run() {
			RangerPolicyEngine policyEngine = plugin.policyEngine;
			if (policyEngine != null) {
				policyEngine.reorderPolicyEvaluators();
			}
		}
	}

	private void syncPoliciesWithAdmin(final DownloadTrigger token) throws InterruptedException{
		policyDownloadQueue.put(token);
		token.waitForCompletion();
	}

	private RangerTagEnricher getTagEnricher() {
		RangerTagEnricher ret = null;
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

}
