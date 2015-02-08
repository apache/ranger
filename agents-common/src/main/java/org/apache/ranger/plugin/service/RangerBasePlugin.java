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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.audit.RangerAuditHandler;
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

	public String getAuditAppType() {
		return auditAppType;
	}

	public String getServiceName() {
		return serviceName;
	}

	public PolicyRefresher getPolicyRefresher() {
		return refresher;
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

		RangerConfiguration.getInstance().addResourcesForServiceType(serviceType);
		RangerConfiguration.getInstance().initAudit(auditAppType);

		serviceName = RangerConfiguration.getInstance().get("ranger.plugin." + serviceType + ".service.name");

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

		String cacheDir          = RangerConfiguration.getInstance().get("ranger.plugin." + serviceType + ".service.store.cache.dir", "/tmp");
		long   pollingIntervalMs = RangerConfiguration.getInstance().getLong("ranger.plugin." + serviceType + ".service.store.pollIntervalMs", 30 * 1000);

		RangerAdminClient admin = new RangerAdminRESTClient();

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

	public boolean grantAccess(GrantRevokeRequest request, RangerAuditHandler auditHandler) {
		boolean ret = false;

		PolicyRefresher refresher = this.refresher;

		if(refresher != null) {
			RangerAdminClient admin = refresher.getRangerAdminClient();
			
			if(admin != null) {
				try {
					admin.grantAccess(serviceName, request);
				} catch(Exception excp) {
					LOG.error("grantAccess() failed", excp);
				}
			}
		}

		return ret;
	}

	public boolean revokeAccess(GrantRevokeRequest request, RangerAuditHandler auditHandler) {
		boolean ret = false;

		PolicyRefresher refresher = this.refresher;

		if(refresher != null) {
			RangerAdminClient admin = refresher.getRangerAdminClient();
			
			if(admin != null) {
				try {
					admin.revokeAccess(serviceName, request);
				} catch(Exception excp) {
					LOG.error("revokeAccess() failed", excp);
				}
			}
		}

		return ret;
	}
}
