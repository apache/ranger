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
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.audit.RangerAuditHandler;
import org.apache.ranger.plugin.contextenricher.RangerContextEnricher;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.PolicyRefresher;
import org.apache.ranger.plugin.util.ServicePolicies;


public class RangerBasePlugin {
	private static final Log LOG = LogFactory.getLog(RangerBasePlugin.class);

	private String                    serviceType  = null;
	private String                    appId        = null;
	private String                    serviceName  = null;
	private PolicyRefresher           refresher    = null;
	private RangerPolicyEngine        policyEngine = null;
	private RangerPolicyEngineOptions policyEngineOptions = new RangerPolicyEngineOptions();
	private RangerAuditHandler        defaultAuditHandler = null;


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

		policyEngineOptions.evaluatorType           = RangerConfiguration.getInstance().get(propertyPrefix + ".policyengine.option.evaluator.type", RangerPolicyEvaluator.EVALUATOR_TYPE_CACHED);
		policyEngineOptions.cacheAuditResults       = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.cache.audit.results", true);
		policyEngineOptions.disableContextEnrichers = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", false);
		policyEngineOptions.disableCustomConditions = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", false);


		RangerAdminClient admin = createAdminClient(propertyPrefix);

		refresher = new PolicyRefresher(this, serviceType, appId, serviceName, admin, pollingIntervalMs, cacheDir);
		refresher.startRefresher();
	}

	public void setPolicies(ServicePolicies policies) {
		RangerPolicyEngine policyEngine = new RangerPolicyEngineImpl(policies, policyEngineOptions);

		this.policyEngine = policyEngine;
	}

	public void cleanup() {
		PolicyRefresher refresher = this.refresher;

		this.serviceName  = null;
		this.policyEngine = null;
		this.refresher    = null;

		if(refresher != null) {
			refresher.stopRefresher();
		}
	}

	public void setDefaultAuditHandler(RangerAuditHandler auditHandler) {
		this.defaultAuditHandler = auditHandler;
	}

	public RangerAuditHandler getDefaultAuditHandler() {
		return this.defaultAuditHandler;
	}

	public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
		return isAccessAllowed(request, defaultAuditHandler);
	}

	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests) {
		return isAccessAllowed(requests, defaultAuditHandler);
	}

	public RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAuditHandler auditHandler) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			enrichRequest(request, policyEngine);

			return policyEngine.isAccessAllowed(request, auditHandler);
		}

		return null;
	}

	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAuditHandler auditHandler) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			enrichRequests(requests, policyEngine);

			return policyEngine.isAccessAllowed(requests, auditHandler);
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

	public void grantAccess(GrantRevokeRequest request, RangerAuditHandler auditHandler) throws Exception {
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
			auditGrantRevoke(request, "grant", isSuccess, auditHandler);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.grantAccess(" + request + ")");
		}
	}

	public void revokeAccess(GrantRevokeRequest request, RangerAuditHandler auditHandler) throws Exception {
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
			auditGrantRevoke(request, "revoke", isSuccess, auditHandler);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.revokeAccess(" + request + ")");
		}
	}


	private RangerAdminClient createAdminClient(String propertyPrefix) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.createAdminClient(" + propertyPrefix + ")");
		}

		RangerAdminClient ret = null;

		String propertyName = propertyPrefix + ".policy.source.impl";
		String policySourceImpl = RangerConfiguration.getInstance().get(propertyName);

		if(StringUtils.isEmpty(policySourceImpl)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Value for property[%s] was null or empty. Unxpected! Will use policy source of type[%s]", propertyName, RangerAdminRESTClient.class.getName()));
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

		ret.init(serviceName, appId, propertyPrefix);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.createAdminClient(" + propertyPrefix + "): policySourceImpl=" + policySourceImpl + ", client=" + ret);
		}
		return ret;
	}

	private void enrichRequest(RangerAccessRequest request, RangerPolicyEngine policyEngine) {
		if(request == null || policyEngine == null) {
			return;
		}

		List<RangerContextEnricher> enrichers = policyEngine.getContextEnrichers();

		if(! CollectionUtils.isEmpty(enrichers)) {
			for(RangerContextEnricher enricher : enrichers) {
				enricher.enrich(request);
			}
		}
	}

	private void enrichRequests(Collection<RangerAccessRequest> requests, RangerPolicyEngine policyEngine) {
		if(CollectionUtils.isEmpty(requests) || policyEngine == null) {
			return;
		}

		List<RangerContextEnricher> enrichers = policyEngine.getContextEnrichers();

		if(! CollectionUtils.isEmpty(enrichers)) {
			for(RangerContextEnricher enricher : enrichers) {
				for(RangerAccessRequest request : requests) {
					enricher.enrich(request);
				}
			}
		}
	}

	private void auditGrantRevoke(GrantRevokeRequest request, String action, boolean isSuccess, RangerAuditHandler auditHandler) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(request != null && auditHandler != null && policyEngine != null) {
			RangerAccessRequestImpl accessRequest = new RangerAccessRequestImpl();
	
			accessRequest.setResource(new RangerAccessResourceImpl(request.getResource()));
			accessRequest.setUser(request.getGrantor());
			accessRequest.setAccessType(RangerPolicyEngine.ADMIN_ACCESS);
			accessRequest.setAction(action);

			// call isAccessAllowed() to determine if audit is enabled or not
			RangerAccessResult accessResult = policyEngine.isAccessAllowed(accessRequest, null);

			if(accessResult != null && accessResult.getIsAudited()) {
				accessRequest.setAccessType(action);
				accessResult.setIsAllowed(isSuccess);

				if(! isSuccess) {
					accessResult.setPolicyId(-1);
				}

				auditHandler.logAudit(accessResult);
			}
		}
	}
}
