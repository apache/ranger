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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

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
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyengine.RangerResourceAccessInfo;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.PolicyRefresher;
import org.apache.ranger.plugin.util.ServicePolicies;


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
	private RangerAuthContext         currentAuthContext;
	private RangerAuthContext         readOnlyAuthContext;
	private RangerAccessResultProcessor resultProcessor;
	private boolean                   useForwardedIPAddress;
	private String[]                  trustedProxyAddresses;
	private Timer                     policyEngineRefreshTimer;
	private RangerAuthContextListener authContextListener;
	private AuditProviderFactory      auditProviderFactory;


	Map<String, LogHistory> logHistoryList = new Hashtable<String, RangerBasePlugin.LogHistory>();
	int logInterval = 30000; // 30 seconds

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
		return new RangerAuthContext(readOnlyAuthContext);
	}

	public RangerAuthContext getCurrentRangerAuthContext() { return currentAuthContext; }

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
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
		clusterName = RangerConfiguration.getInstance().get(propertyPrefix + ".ambari.cluster.name", "");

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

		policyEngineOptions.configureForPlugin(configuration, propertyPrefix);

		LOG.info(policyEngineOptions);

		servicePluginMap.put(serviceName, this);

		RangerAdminClient admin = createAdminClient(serviceName, appId, propertyPrefix);

		refresher = new PolicyRefresher(this, serviceType, appId, serviceName, admin, pollingIntervalMs, cacheDir);
		refresher.setDaemon(true);
		refresher.startRefresher();

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

		// guard against catastrophic failure during policy engine Initialization or
		try {
			RangerPolicyEngine oldPolicyEngine = this.policyEngine;

			if (policies == null) {
				policies = getDefaultSvcPolicies();
			}
			if (policies == null) {
				this.policyEngine = null;
				readOnlyAuthContext = null;
			} else {
				currentAuthContext = new RangerAuthContext();
				RangerPolicyEngine policyEngine = new RangerPolicyEngineImpl(appId, policies, policyEngineOptions);
				policyEngine.setUseForwardedIPAddress(useForwardedIPAddress);
				policyEngine.setTrustedProxyAddresses(trustedProxyAddresses);
				this.policyEngine = policyEngine;
				currentAuthContext.setPolicyEngine(this.policyEngine);
				readOnlyAuthContext = new RangerAuthContext(currentAuthContext);
			}
			contextChanged();

			if (oldPolicyEngine != null && !oldPolicyEngine.preCleanup()) {
				LOG.error("preCleanup() failed on the previous policy engine instance !!");
			}
		} catch (Exception e) {
			LOG.error("setPolicies: policy engine initialization failed!  Leaving current policy engine as-is. Exception : ", e);
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

		String serviceName = this.serviceName;

		this.serviceName  = null;
		this.policyEngine = null;
		this.refresher    = null;
		this.policyEngineRefreshTimer = null;

		if (refresher != null) {
			refresher.stopRefresher();
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

	public void grantAccess(GrantRevokeRequest request, RangerAccessResultProcessor resultProcessor) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBasePlugin.grantAccess(" + request + ")");
		}

		PolicyRefresher   refresher = this.refresher;
		RangerAdminClient admin     = refresher == null ? null : refresher.getRangerAdminClient();
		boolean           isSuccess = false;

		try {
			if(admin == null) {
				throw new Exception("ranger-admin client is null");
			}

			if (policyEngine != null) {
				request.setZoneName(policyEngine.getMatchedZoneName(request));
			}

			admin.grantAccess(request);

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

		PolicyRefresher   refresher = this.refresher;
		RangerAdminClient admin     = refresher == null ? null : refresher.getRangerAdminClient();
		boolean           isSuccess = false;

		try {
			if(admin == null) {
				throw new Exception("ranger-admin client is null");
			}

			if (policyEngine != null) {
				request.setZoneName(policyEngine.getMatchedZoneName(request));
			}

			admin.revokeAccess(request);

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
			accessRequest.setClusterName(request.getClusterName());

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

	public RangerServiceDef getDefaultServiceDef() {
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

	static class LogHistory {
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
}
