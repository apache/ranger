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
import java.util.Map;

import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResult.Result;

public class RangerHiveAuditHandler extends RangerDefaultAuditHandler {
	private static final String RangerModuleName =  RangerConfiguration.getInstance().get(RangerHadoopConstants.AUDITLOG_RANGER_MODULE_ACL_NAME_PROP , RangerHadoopConstants.DEFAULT_RANGER_MODULE_ACL_NAME) ;

	Collection<AuthzAuditEvent> auditEvents  = null;
	boolean                     deniedExists = false;

	public RangerHiveAuditHandler() {
		super();
	}

	@Override
	public void logAudit(RangerAccessResult result) {
		if(! result.getIsAudited()) {
			return;
		}

		AuthzAuditEvent auditEvent = new AuthzAuditEvent();

		RangerHiveAccessRequest request  = (RangerHiveAccessRequest)result.getAccessRequest();
		RangerHiveResource      resource = (RangerHiveResource)request.getResource();
		boolean                 isAllowed = result.getResult() == Result.ALLOWED;

		auditEvent.setAclEnforcer(RangerModuleName);
		auditEvent.setSessionId(request.getSessionId());
		auditEvent.setResourceType("@" + StringUtil.toLower(resource.getObjectType().name())); // to be consistent with earlier release
		auditEvent.setAccessType(request.getAccessType().toString());
		auditEvent.setAction(request.getAction());
		auditEvent.setUser(request.getUser());
		auditEvent.setAccessResult((short)(isAllowed ? 1 : 0));
		auditEvent.setPolicyId(result.getPolicyId());
		auditEvent.setClientIP(request.getClientIPAddress());
		auditEvent.setClientType(request.getClientType());
		auditEvent.setEventTime(request.getAccessTime());
		auditEvent.setRepositoryType(result.getServiceType());
		auditEvent.setRepositoryName(result.getServiceName()) ;
		auditEvent.setRequestData(request.getRequestData());
		auditEvent.setResourcePath(getResourceValueAsString(resource, result.getServiceDef()));

		addAuthzAuditEvent(auditEvent);
	}

	/*
	 * This method is expected to be called ONLY to process the results for multiple-columns in a table.
	 * To ensure this, RangerHiveAuthorizer should call isAccessAllowed(Collection<requests>) only for this condition
	 */
	@Override
	public void logAudit(Collection<RangerAccessResult> results) {
		Map<Long, AuthzAuditEvent> auditEvents = new HashMap<Long, AuthzAuditEvent>();

		for(RangerAccessResult result : results) {
			if(! result.getIsAudited()) {
				continue;
			}

			RangerHiveAccessRequest request    = (RangerHiveAccessRequest)result.getAccessRequest();
			RangerHiveResource      resource   = (RangerHiveResource)request.getResource();
			boolean                 isAllowed  = result.getResult() == Result.ALLOWED;
			AuthzAuditEvent         auditEvent = auditEvents.get(result.getPolicyId());

			if(auditEvent == null) {
				auditEvent = new AuthzAuditEvent();
				auditEvents.put(result.getPolicyId(), auditEvent);

				auditEvent.setAclEnforcer(RangerModuleName);
				auditEvent.setSessionId(request.getSessionId());
				auditEvent.setResourceType("@" + StringUtil.toLower(resource.getObjectType().name())); // to be consistent with earlier release
				auditEvent.setAccessType(request.getAccessType().toString());
				auditEvent.setAction(request.getAction());
				auditEvent.setUser(request.getUser());
				auditEvent.setAccessResult((short)(isAllowed ? 1 : 0));
				auditEvent.setPolicyId(result.getPolicyId());
				auditEvent.setClientIP(request.getClientIPAddress());
				auditEvent.setClientType(request.getClientType());
				auditEvent.setEventTime(request.getAccessTime());
				auditEvent.setRepositoryType(result.getServiceType());
				auditEvent.setRepositoryName(result.getServiceName()) ;
				auditEvent.setRequestData(request.getRequestData());
				auditEvent.setResourcePath(getResourceValueAsString(resource, result.getServiceDef()));
			} else if(isAllowed){
				auditEvent.setResourcePath(auditEvent.getResourcePath() + "," + resource.getColumn());
			} else {
				auditEvent.setResourcePath(getResourceValueAsString(resource, result.getServiceDef()));
			}
			
			if(!isAllowed) {
				auditEvent.setResourcePath(getResourceValueAsString(resource, result.getServiceDef()));

				break;
			}
		}

		for(AuthzAuditEvent auditEvent : auditEvents.values()) {
			addAuthzAuditEvent(auditEvent);
		}
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
