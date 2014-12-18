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

package org.apache.ranger.plugin.policyengine;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.manager.ServiceDefManager;
import org.apache.ranger.plugin.manager.ServiceManager;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;


public class RangerPolicyEngineImpl implements RangerPolicyEngine {
	private static final Log LOG = LogFactory.getLog(RangerPolicyEngineImpl.class);

	private String             svcName    = null;
	private ServiceDefManager  sdMgr      = null;
	private ServiceManager     svcMgr     = null;
	private RangerService      service    = null;
	private RangerServiceDef   serviceDef = null;
	private List<RangerPolicy> policies   = null;

	public RangerPolicyEngineImpl() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngine()");
		}

		sdMgr  = new ServiceDefManager();
		svcMgr = new ServiceManager();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngine()");
		}
	}
	
	public void init(String serviceName) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngine.init(" + serviceName + ")");
		}

		svcName = serviceName;
		service = svcMgr.getByName(svcName);

		if(service == null) {
			LOG.error(svcName + ": service not found");
		} else {
			serviceDef = sdMgr.getByName(service.getType());

			if(serviceDef == null) {
				String msg = service.getType() + ": service-def not found";

				LOG.error(msg);

				throw new Exception(msg);
			}

			policies = svcMgr.getPolicies(service.getId());

			if(LOG.isDebugEnabled()) {
				LOG.debug("found " + (policies == null ? 0 : policies.size()) + " policies in service '" + svcName + "'");
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngine.init(" + serviceName + ")");
		}
	}

	@Override
	public boolean isAccessAllowed(RangerAccessRequest request) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isAccessAllowed(List<RangerAccessRequest> requests,
			List<Boolean> results) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void auditAccess(RangerAccessRequest request) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void auditAccess(List<RangerAccessRequest> requests,
			List<Boolean> results) {
		// TODO Auto-generated method stub
		
	}
}
