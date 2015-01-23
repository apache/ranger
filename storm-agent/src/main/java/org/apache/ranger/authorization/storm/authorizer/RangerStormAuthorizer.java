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

 package org.apache.ranger.authorization.storm.authorizer;

import java.security.Principal;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.audit.model.EnumRepositoryType;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.authorization.storm.RangerStormAccessVerifier;
import org.apache.ranger.authorization.storm.RangerStormAccessVerifierFactory;
import org.apache.ranger.authorization.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.security.auth.IAuthorizer;
import backtype.storm.security.auth.ReqContext;

public class RangerStormAuthorizer implements IAuthorizer {

	private static final Logger LOG = LoggerFactory.getLogger(RangerStormAuthorizer.class);
	
	private static final String RangerModuleName =  RangerConfiguration.getInstance().get(RangerHadoopConstants.AUDITLOG_RANGER_MODULE_ACL_NAME_PROP , RangerHadoopConstants.DEFAULT_RANGER_MODULE_ACL_NAME) ;
	
	private static final String repositoryName     = RangerConfiguration.getInstance().get(RangerHadoopConstants.AUDITLOG_REPOSITORY_NAME_PROP);
	
	private RangerStormAccessVerifier rangerVerifier = RangerStormAccessVerifierFactory.getInstance() ;

	static {
		RangerConfiguration.getInstance().initAudit("storm");
	}


	/**
     * permit() method is invoked for each incoming Thrift request.
     * @param context request context includes info about 
     * @param operation operation name
     * @param topology_storm configuration of targeted topology 
     * @return true if the request is authorized, false if reject
     */
	
	@Override
	public boolean permit(ReqContext aRequestContext, String aOperationName, Map aTopologyConfigMap) {
		
		boolean accessAllowed = false ;
		
		String topologyName = null ;
		
		try {
			topologyName = (aTopologyConfigMap == null ? "" : (String)aTopologyConfigMap.get(Config.TOPOLOGY_NAME)) ;
	
			if (LOG.isDebugEnabled()) {
				LOG.debug("[req "+ aRequestContext.requestID()+ "] Access "
		                + " from: [" + aRequestContext.remoteAddress() + "]"
		                + " user: [" + aRequestContext.principal() + "],"  
		                + " op:   [" + aOperationName + "],"
		                + "topology: [" + topologyName + "]") ;
				
				if (aTopologyConfigMap != null) {
					for(Object keyObj : aTopologyConfigMap.keySet()) {
						Object valObj = aTopologyConfigMap.get(keyObj) ;
						LOG.debug("TOPOLOGY CONFIG MAP [" + keyObj + "] => [" + valObj + "]");
					}
				}
				else {
					LOG.debug("TOPOLOGY CONFIG MAP is passed as null.") ;
				}
			}
	
			String userName = null ;
			String[] groups = null ;
	
			Principal user = aRequestContext.principal() ;
			
			if (user != null) {
				userName = user.getName() ;
				if (userName != null) {
					UserGroupInformation ugi = UserGroupInformation.createRemoteUser(userName) ;
					userName = ugi.getShortUserName() ;
					groups = ugi.getGroupNames() ;
					if (LOG.isDebugEnabled()) {
						LOG.debug("User found from principal [" + user.getName() + "] => user:[" + userName + "], groups:[" + StringUtil.toString(groups) + "]") ;
					}

				}
			}
				
				
			if (userName != null) {
				accessAllowed = rangerVerifier.isAccessAllowed(userName, groups, aOperationName, topologyName) ;
				if (LOG.isDebugEnabled()) {
					LOG.debug("User found from principal [" + userName + "], groups [" + StringUtil.toString(groups) + "]: verifying using [" + rangerVerifier.getClass().getName() + "], allowedFlag => [" + accessAllowed + "]");
				}
			}
			else {
				LOG.info("NULL User found from principal [" + user + "]: Skipping authorization;  allowedFlag => [" + accessAllowed + "]");
			}
				
			boolean isAuditEnabled = rangerVerifier.isAudited(topologyName) ;
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("User found from principal [" + userName + "] and verifying using [" + rangerVerifier + "], Audit Enabled:" + isAuditEnabled);
			}
			
			if (isAuditEnabled) {
				
				AuthzAuditEvent auditEvent = new AuthzAuditEvent() ;
	
				String sessionId = null ;
				String clientIp = null ;
				
				if (aRequestContext != null) {
					sessionId = String.valueOf(aRequestContext.requestID()) ;
					clientIp =  (aRequestContext.remoteAddress() == null ? null : aRequestContext.remoteAddress().getHostAddress() ) ;
				}
				
				try {
					auditEvent.setAclEnforcer(RangerModuleName);
					auditEvent.setSessionId(sessionId);
					auditEvent.setResourceType("@ TOPOLOGY"); 
					auditEvent.setAccessType(aOperationName) ;
					auditEvent.setAction(aOperationName);
					auditEvent.setUser(userName);
					auditEvent.setAccessResult((short)(accessAllowed ? 1 : 0));
					auditEvent.setClientIP(clientIp);
					auditEvent.setClientType("Strom REST");
					auditEvent.setEventTime(StringUtil.getUTCDate());
					auditEvent.setRepositoryType(EnumRepositoryType.STORM);
					auditEvent.setRepositoryName(repositoryName) ;
					auditEvent.setRequestData("");
	
					auditEvent.setResourcePath(topologyName);
				
					if(LOG.isDebugEnabled()) {
						LOG.debug("logAuditEvent [" + auditEvent + "] - START");
					}
	
					AuditProviderFactory.getAuditProvider().log(auditEvent);
	
					if(LOG.isDebugEnabled()) {
						LOG.debug("logAuditEvent [" + auditEvent + "] - END");
					}
				}
				catch(Throwable t) {
					LOG.error("ERROR logEvent [" + auditEvent + "]", t);
				}
					
			}
		}
		catch(Throwable t) {
			LOG.error("RangerStormAuthorizer found this exception", t);
		}
		finally {
			if (LOG.isDebugEnabled()) {
				LOG.debug("[req "+ aRequestContext.requestID()+ "] Access "
		                + " from: [" + aRequestContext.remoteAddress() + "]"
		                + " user: [" + aRequestContext.principal() + "],"  
		                + " op:   [" + aOperationName + "],"
		                + "topology: [" + topologyName + "] => returns [" + accessAllowed + "]") ;
			}
		}
		
		return accessAllowed ;
	}
	
	/**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     */

	@Override
	public void prepare(Map aStormConfigMap) {
	}
	
}
