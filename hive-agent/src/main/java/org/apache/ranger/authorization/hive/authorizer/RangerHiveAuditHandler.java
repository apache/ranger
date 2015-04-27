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

package org.apache.ranger.authorization.hive.authorizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;

import com.google.common.collect.Lists;

public class RangerHiveAuditHandler extends RangerDefaultAuditHandler {
	private static final String RangerModuleName =  RangerConfiguration.getInstance().get(RangerHadoopConstants.AUDITLOG_RANGER_MODULE_ACL_NAME_PROP , RangerHadoopConstants.DEFAULT_RANGER_MODULE_ACL_NAME) ;

	Collection<AuthzAuditEvent> auditEvents  = null;
	boolean                     deniedExists = false;

	public RangerHiveAuditHandler() {
		super();
	}
	
	AuthzAuditEvent createAuditEvent(RangerAccessResult result, String accessType, String resourcePath) {
		RangerAccessRequest  request      = result.getAccessRequest();
		RangerAccessResource resource     = request.getResource();
		String               resourceType = resource != null ? resource.getLeafName(result.getServiceDef()) : null;

		AuthzAuditEvent auditEvent = new AuthzAuditEvent();
		auditEvent.setAclEnforcer(RangerModuleName);
		auditEvent.setSessionId(request.getSessionId());
		auditEvent.setResourceType("@" + resourceType); // to be consistent with earlier release
		auditEvent.setAccessType(accessType);
		auditEvent.setAction(request.getAction());
		auditEvent.setUser(request.getUser());
		auditEvent.setAccessResult((short)(result.getIsAllowed() ? 1 : 0));
		auditEvent.setPolicyId(result.getPolicyId());
		auditEvent.setClientIP(request.getClientIPAddress());
		auditEvent.setClientType(request.getClientType());
		auditEvent.setEventTime(request.getAccessTime());
		auditEvent.setRepositoryType(result.getServiceType());
		auditEvent.setRepositoryName(result.getServiceName()) ;
		auditEvent.setRequestData(request.getRequestData());
		auditEvent.setResourcePath(resourcePath);

		return auditEvent;
	}
	
	AuthzAuditEvent createAuditEvent(RangerAccessResult result) {
		RangerAccessRequest  request  = result.getAccessRequest();
		RangerAccessResource resource = request.getResource();

		String accessType = null;
		if(request instanceof RangerHiveAccessRequest) {
			RangerHiveAccessRequest hiveRequest = (RangerHiveAccessRequest)request;

			accessType = hiveRequest.getHiveAccessType().toString();
		}

		if(StringUtils.isEmpty(accessType)) {
			accessType = request.getAccessType();
		}

		String resourcePath = resource != null ? resource.getAsString(result.getServiceDef()) : null;

		return createAuditEvent(result, accessType, resourcePath);
	}

	public List<AuthzAuditEvent> createAuditEvents(Collection<RangerAccessResult> results) {

		Map<Long, AuthzAuditEvent> auditEvents = new HashMap<Long, AuthzAuditEvent>();
		Iterator<RangerAccessResult> iterator = results.iterator();
		AuthzAuditEvent deniedAuditEvent = null;
		while (iterator.hasNext() && deniedAuditEvent == null) {
			RangerAccessResult result = iterator.next();
			if(result.getIsAudited()) {
				if (!result.getIsAllowed()) {
					deniedAuditEvent = createAuditEvent(result); 
				} else {
					long policyId = result.getPolicyId();
					if (auditEvents.containsKey(policyId)) { // add this result to existing event by updating column values
						AuthzAuditEvent auditEvent = auditEvents.get(policyId);
						RangerHiveAccessRequest request    = (RangerHiveAccessRequest)result.getAccessRequest();
						RangerHiveResource resource   = (RangerHiveResource)request.getResource();
						String resourcePath = auditEvent.getResourcePath() + "," + resource.getColumn(); 
						auditEvent.setResourcePath(resourcePath);
					} else { // new event as this approval was due to a different policy.
						AuthzAuditEvent auditEvent = createAuditEvent(result);
						auditEvents.put(policyId, auditEvent);
					}
				}
			}
		}
		List<AuthzAuditEvent> result;
		if (deniedAuditEvent == null) {
			result = new ArrayList<>(auditEvents.values());
		} else {
			result = Lists.newArrayList(deniedAuditEvent);
		}
		
		return result;
	}
	
	@Override
	public void processResult(RangerAccessResult result) {
		if(! result.getIsAudited()) {
			return;
		}
		AuthzAuditEvent auditEvent = createAuditEvent(result);
		addAuthzAuditEvent(auditEvent);
	}

	/**
	 * This method is expected to be called ONLY to process the results for multiple-columns in a table.
	 * To ensure this, RangerHiveAuthorizer should call isAccessAllowed(Collection<requests>) only for this condition
	 */
	@Override
	public void processResults(Collection<RangerAccessResult> results) {
		List<AuthzAuditEvent> auditEvents = createAuditEvents(results);
		for(AuthzAuditEvent auditEvent : auditEvents) {
			addAuthzAuditEvent(auditEvent);
		}
	}

    public void logAuditEventForFiltering(RangerAccessResult result, HiveOperationType hiveOpType) {
		
		if(! result.getIsAudited()) {
			return;
		}
		
		RangerHiveAccessRequest request  = (RangerHiveAccessRequest)result.getAccessRequest();
		RangerHiveResource      resource = (RangerHiveResource)request.getResource();
		String resourcePath = resource.getObjectType().toString();
    	String accessType = getAccessTypeForMetaOperation(hiveOpType);
		
    	AuthzAuditEvent auditEvent = createAuditEvent(result, accessType, resourcePath);

		addAuthzAuditEvent(auditEvent);
    }

	String getAccessTypeForMetaOperation(HiveOperationType hiveOperationType) {
		String result;
		switch (hiveOperationType) {
		case SHOWDATABASES:
			result = "SHOW DATABASES";
			break;
		case SHOWTABLES:
			result = "SHOW TABLES";
			break;
		default:
			result = "OTHER METADATA OP";
			break;
		}
		return result;
	}

	public void logAuditEventForDfs(String userName, String dfsCommand, boolean accessGranted, int repositoryType, String repositoryName) {
		AuthzAuditEvent auditEvent = new AuthzAuditEvent();

		auditEvent.setAclEnforcer(RangerModuleName);
		auditEvent.setResourceType("@dfs"); // to be consistent with earlier release
		auditEvent.setAccessType("DFS");
		auditEvent.setAction("DFS");
		auditEvent.setUser(userName);
		auditEvent.setAccessResult((short)(accessGranted ? 1 : 0));
		auditEvent.setEventTime(StringUtil.getUTCDate());
		auditEvent.setRepositoryType(repositoryType);
		auditEvent.setRepositoryName(repositoryName) ;
		auditEvent.setRequestData(dfsCommand);

		auditEvent.setResourcePath(dfsCommand);

		addAuthzAuditEvent(auditEvent);
    }

    public void flushAudit() {
    	if(auditEvents == null) {
    		return;
    	}

    	for(AuthzAuditEvent auditEvent : auditEvents) {
    		if(deniedExists && auditEvent.getAccessResult() != 0) { // if deny exists, skip logging for allowed results
    			continue;
    		}

    		super.logAuthzAudit(auditEvent);
    	}
    }

    private void addAuthzAuditEvent(AuthzAuditEvent auditEvent) {
    	if(auditEvent != null) {
    		if(auditEvents == null) {
    			auditEvents = new ArrayList<AuthzAuditEvent>();
    		}
    		
    		auditEvents.add(auditEvent);
    		
    		if(auditEvent.getAccessResult() == 0) {
    			deniedExists = true;
    		}
    	}
    }
}
