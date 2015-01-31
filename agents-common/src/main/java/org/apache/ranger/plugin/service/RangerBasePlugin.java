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

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.audit.RangerAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.store.ServiceStoreFactory;
import org.apache.ranger.plugin.util.PolicyRefresher;


public class RangerBasePlugin {
	private String             serviceType  = null;
	private String             serviceName  = null;
	private RangerPolicyEngine policyEngine = null;
	private PolicyRefresher    refresher    = null;


	public RangerBasePlugin(String serviceType) {
		this.serviceType = serviceType;
	}

	public String getServiceType() {
		return serviceType;
	}

	public String getServiceName() {
		return serviceName;
	}

	public RangerPolicyEngine getPolicyEngine() {
		return policyEngine;
	}

	public void init() {
		RangerPolicyEngine policyEngine = new RangerPolicyEngineImpl();
		
		init(policyEngine);
	}

	public synchronized void init(RangerPolicyEngine policyEngine) {
		cleanup();


		String serviceName       = RangerConfiguration.getInstance().get("ranger.plugin." + serviceType + ".service.name");
		String serviceStoreClass = RangerConfiguration.getInstance().get("ranger.plugin." + serviceType + ".service.store.class", "org.apache.ranger.plugin.store.rest.ServiceRESTStore");
		String cacheDir          = RangerConfiguration.getInstance().get("ranger.plugin." + serviceType + ".service.store.cache.dir", "/tmp");
		long   pollingIntervalMs = RangerConfiguration.getInstance().getLong("ranger.plugin." + serviceType + ".service.store.pollIntervalMs", 30 * 1000);

		if(StringUtils.isEmpty(serviceName)) {
			// get the serviceName from download URL: http://ranger-admin-host:port/service/assets/policyList/serviceName
			String policyDownloadUrl = RangerConfiguration.getInstance().get("xasecure." + serviceType + ".policymgr.url");

			if(! StringUtils.isEmpty(policyDownloadUrl)) {
				int idx = policyDownloadUrl.lastIndexOf('/');

				if(idx != -1) {
					serviceName = policyDownloadUrl.substring(idx + 1);
				}
			}
		}

		ServiceStore serviceStore = ServiceStoreFactory.instance().getServiceStore(serviceStoreClass);

		refresher = new PolicyRefresher(policyEngine, serviceType, serviceName, serviceStore, pollingIntervalMs, cacheDir);
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


	public RangerAccessResult createAccessResult(RangerAccessRequest request) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			return policyEngine.createAccessResult(request);
		}

		return null;
	}


	public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			return policyEngine.isAccessAllowed(request);
		}

		return null;
	}


	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			return policyEngine.isAccessAllowed(requests);
		}

		return null;
	}


	public RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAuditHandler auditHandler) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			return policyEngine.isAccessAllowed(request, auditHandler);
		}

		return null;
	}


	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAuditHandler auditHandler) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			return policyEngine.isAccessAllowed(requests, auditHandler);
		}

		return null;
	}
}
