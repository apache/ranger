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

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.*;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.PolicyRefresher;
import org.apache.ranger.plugin.util.ServicePolicies;


public class RangerBasePlugin {
	private static final Log LOG = LogFactory.getLog(RangerBasePlugin.class);

	public static final char RANGER_TRUSTED_PROXY_IPADDRESSES_SEPARATOR_CHAR = ',';

	private String                    serviceType  = null;
	private String                    appId        = null;
	private String                    serviceName  = null;
	private PolicyRefresher           refresher    = null;
	private RangerPolicyEngine        policyEngine = null;
	private RangerPolicyEngineOptions policyEngineOptions = new RangerPolicyEngineOptions();
	private RangerAccessResultProcessor resultProcessor = null;
	private boolean                   useForwardedIPAddress = false;
	private String[]                  trustedProxyAddresses = null;

	Map<String, LogHistory> logHistoryList = new Hashtable<String, RangerBasePlugin.LogHistory>();
	int logInterval = 30000; // 30 seconds


	public RangerBasePlugin(String serviceType, String appId) {
		this.serviceType = serviceType;
		this.appId       = appId;
	}

	public String getServiceType() {
		return serviceType;
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

	public void init() {
		cleanup();

		RangerConfiguration.getInstance().addResourcesForServiceType(serviceType);
		RangerConfiguration.getInstance().initAudit(appId);

		String propertyPrefix    = "ranger.plugin." + serviceType;
		long   pollingIntervalMs = RangerConfiguration.getInstance().getLong(propertyPrefix + ".policy.pollIntervalMs", 30 * 1000);
		String cacheDir          = RangerConfiguration.getInstance().get(propertyPrefix + ".policy.cache.dir");
		serviceName = RangerConfiguration.getInstance().get(propertyPrefix + ".service.name");

		useForwardedIPAddress = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".use.x-forwarded-for.ipaddress", false);
		String trustedProxyAddressString = RangerConfiguration.getInstance().get(propertyPrefix + ".trusted.proxy.ipaddresses");
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

		policyEngineOptions.evaluatorType           = RangerConfiguration.getInstance().get(propertyPrefix + ".policyengine.option.evaluator.type", RangerPolicyEvaluator.EVALUATOR_TYPE_AUTO);
		policyEngineOptions.cacheAuditResults       = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.cache.audit.results", true);
		policyEngineOptions.disableContextEnrichers = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", false);
		policyEngineOptions.disableCustomConditions = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", false);
		policyEngineOptions.disableTagPolicyEvaluation = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.disable.tagpolicy.evaluation", false);

		RangerAdminClient admin = createAdminClient(serviceName, appId, propertyPrefix);

		refresher = new PolicyRefresher(this, serviceType, appId, serviceName, admin, pollingIntervalMs, cacheDir);
		refresher.setDaemon(true);
		refresher.startRefresher();
	}

	public void setPolicies(ServicePolicies policies) {

		// guard against catastrophic failure during policy engine Initialization or
		try {
			RangerPolicyEngine oldPolicyEngine = this.policyEngine;

			RangerPolicyEngine policyEngine = new RangerPolicyEngineImpl(appId, policies, policyEngineOptions);
			policyEngine.setUseForwardedIPAddress(useForwardedIPAddress);
			policyEngine.setTrustedProxyAddresses(trustedProxyAddresses);

			this.policyEngine = policyEngine;

			if (oldPolicyEngine != null && !oldPolicyEngine.preCleanup()) {
				LOG.error("preCleanup() failed on the previous policy engine instance !!");
			}
		} catch (Exception e) {
			LOG.error("setPolicies: policy engine initialization failed!  Leaving current policy engine as-is.");
		}
	}

	public void cleanup() {

		PolicyRefresher refresher = this.refresher;

		RangerPolicyEngine policyEngine = this.policyEngine;

		this.serviceName  = null;
		this.policyEngine = null;
		this.refresher    = null;

		if (refresher != null) {
			refresher.stopRefresher();
		}

		if (policyEngine != null) {
			policyEngine.cleanup();
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

			return policyEngine.isAccessAllowed(request, resultProcessor);
		}

		return null;
	}

	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAccessResultProcessor resultProcessor) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			policyEngine.preProcess(requests);

			return policyEngine.isAccessAllowed(requests, resultProcessor);
		}

		return null;
	}

	public RangerDataMaskResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			policyEngine.preProcess(request);

			return policyEngine.evalDataMaskPolicies(request, resultProcessor);
		}

		return null;
	}

	public RangerRowFilterResult evalRowFilterPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			policyEngine.preProcess(request);

			return policyEngine.evalRowFilterPolicies(request, resultProcessor);
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

	public RangerAccessResult createAccessResult(RangerAccessRequest request) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			return policyEngine.createAccessResult(request);
		}

		return null;
	}

	public void grantAccess(GrantRevokeRequest request, RangerAccessResultProcessor resultProcessor) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.grantAccess(" + request + ")");
		}

		PolicyRefresher   refresher = this.refresher;
		RangerAdminClient admin     = refresher == null ? null : refresher.getRangerAdminClient();
		boolean           isSuccess = false;

		try {
			if(admin == null) {
				throw new Exception("ranger-admin client is null");
			}

			admin.grantAccess(request);

			isSuccess = true;
		} finally {
			auditGrantRevoke(request, "grant", isSuccess, resultProcessor);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.grantAccess(" + request + ")");
		}
	}

	public void revokeAccess(GrantRevokeRequest request, RangerAccessResultProcessor resultProcessor) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.revokeAccess(" + request + ")");
		}

		PolicyRefresher   refresher = this.refresher;
		RangerAdminClient admin     = refresher == null ? null : refresher.getRangerAdminClient();
		boolean           isSuccess = false;

		try {
			if(admin == null) {
				throw new Exception("ranger-admin client is null");
			}

			admin.revokeAccess(request);

			isSuccess = true;
		} finally {
			auditGrantRevoke(request, "revoke", isSuccess, resultProcessor);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.revokeAccess(" + request + ")");
		}
	}

	public static RangerAdminClient createAdminClient(String rangerServiceName, String applicationId, String propertyPrefix) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.createAdminClient(" + rangerServiceName + ", " + applicationId + ", " + propertyPrefix + ")");
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
			LOG.debug("<== RangerAdminRESTClient.createAdminClient(" + rangerServiceName + ", " + applicationId + ", " + propertyPrefix + "): policySourceImpl=" + policySourceImpl + ", client=" + ret);
		}
		return ret;
	}

	private void auditGrantRevoke(GrantRevokeRequest request, String action, boolean isSuccess, RangerAccessResultProcessor resultProcessor) {
		if(request != null && resultProcessor != null) {
			RangerAccessRequestImpl accessRequest = new RangerAccessRequestImpl();
	
			accessRequest.setResource(new RangerAccessResourceImpl(request.getResource()));
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
		long lastLogTime = 0;
		int counter=0;
	}

}
