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
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.PolicyRefresher;


public class RangerBasePlugin {
	private static final Log LOG = LogFactory.getLog(RangerBasePlugin.class);

	private String             serviceType  = null;
	private String             auditAppType = null;
	private String             serviceName  = null;
	private PolicyRefresher    refresher    = null;
	private RangerPolicyEngine policyEngine = null;


	public RangerBasePlugin(String serviceType, String auditAppType) {
		this.serviceType  = serviceType;
		this.auditAppType = auditAppType;
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

	public String getAuditAppType() {
		return auditAppType;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void init() {
		RangerPolicyEngine policyEngine = new RangerPolicyEngineImpl();
		
		init(policyEngine);
	}

	public synchronized void init(RangerPolicyEngine policyEngine) {
		cleanup();

		RangerConfiguration.getInstance().addResourcesForServiceType(serviceType);
		RangerConfiguration.getInstance().initAudit(auditAppType);

		String propertyPrefix    = "ranger.plugin." + serviceType;
		long   pollingIntervalMs = RangerConfiguration.getInstance().getLong(propertyPrefix + ".policy.pollIntervalMs", 30 * 1000);
		String cacheDir          = RangerConfiguration.getInstance().get(propertyPrefix + ".policy.cache.dir");

		serviceName = RangerConfiguration.getInstance().get(propertyPrefix + ".service.name");

		RangerAdminClient admin = createAdminClient(propertyPrefix);

		refresher = new PolicyRefresher(policyEngine, serviceType, serviceName, admin, pollingIntervalMs, cacheDir);
		refresher.startRefresher();
		this.policyEngine = policyEngine;
	}

	public synchronized void cleanup() {
		PolicyRefresher refresher = this.refresher;

		this.serviceName  = null;
		this.policyEngine = null;
		this.refresher    = null;

		if(refresher != null) {
			refresher.stopRefresher();
		}
	}

	public void setDefaultAuditHandler(RangerAuditHandler auditHandler) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			policyEngine.setDefaultAuditHandler(auditHandler);
		}
	}

	public RangerAuditHandler getDefaultAuditHandler() {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			return policyEngine.getDefaultAuditHandler();
		}

		return null;
	}

	public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			enrichRequest(request);

			return policyEngine.isAccessAllowed(request);
		}

		return null;
	}


	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			enrichRequests(requests);

			return policyEngine.isAccessAllowed(requests);
		}

		return null;
	}


	public RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAuditHandler auditHandler) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			enrichRequest(request);

			return policyEngine.isAccessAllowed(request, auditHandler);
		}

		return null;
	}


	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAuditHandler auditHandler) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			enrichRequests(requests);

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
		PolicyRefresher   refresher = this.refresher;
		RangerAdminClient admin     = refresher == null ? null : refresher.getRangerAdminClient();

		if(admin == null) {
			throw new Exception("ranger-admin client is null");
		}

		admin.grantAccess(serviceName, request);
	}

	public void revokeAccess(GrantRevokeRequest request, RangerAuditHandler auditHandler) throws Exception {
		PolicyRefresher   refresher = this.refresher;
		RangerAdminClient admin     = refresher == null ? null : refresher.getRangerAdminClient();

		if(admin == null) {
			throw new Exception("ranger-admin client is null");
		}

		admin.revokeAccess(serviceName, request);
	}


	private RangerAdminClient createAdminClient(String propertyPrefix) {
		RangerAdminClient ret = null;

		String policySourceImpl = RangerConfiguration.getInstance().get(propertyPrefix + ".source.impl");

		if(!StringUtils.isEmpty(policySourceImpl)) {
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

		ret.init(propertyPrefix);

		return ret;
	}

	private void enrichRequest(RangerAccessRequest request) {
		if(request == null) {
			return;
		}

		RangerPolicyEngine          policyEngine = this.policyEngine;
		List<RangerContextEnricher> enrichers    = policyEngine != null ? policyEngine.getContextEnrichers() : null;

		if(! CollectionUtils.isEmpty(enrichers)) {
			for(RangerContextEnricher enricher : enrichers) {
				enricher.enrich(request);
			}
		}
	}

	private void enrichRequests(Collection<RangerAccessRequest> requests) {
		if(CollectionUtils.isEmpty(requests)) {
			return;
		}

		RangerPolicyEngine          policyEngine = this.policyEngine;
		List<RangerContextEnricher> enrichers    = policyEngine != null ? policyEngine.getContextEnrichers() : null;

		if(! CollectionUtils.isEmpty(enrichers)) {
			for(RangerContextEnricher enricher : enrichers) {
				for(RangerAccessRequest request : requests) {
					enricher.enrich(request);
				}
			}
		}
	}
}
