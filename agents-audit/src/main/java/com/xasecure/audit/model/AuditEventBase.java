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

 package com.xasecure.audit.model;

import java.util.Date;
import java.util.UUID;

import com.xasecure.audit.dao.DaoManager;



public abstract class AuditEventBase {
	protected static String FIELD_SEPARATOR = ";";
	
	protected static final int MAX_ACTION_FIELD_SIZE = 1800 ;
	protected static final int MAX_REQUEST_DATA_FIELD_SIZE = 1800 ;

	
	protected String agentId        = null;
	protected String user           = null;
	protected Date   eventTime      = new Date();
	protected long   policyId       = 0;
	protected String accessType     = null;
	protected short  accessResult   = 0; // 0 - DENIED; 1 - ALLOWED; HTTP return code 
	protected String resultReason   = null;
	protected String aclEnforcer    = null;
	protected int    repositoryType = 0;
	protected String repositoryName = null;
	protected String sessionId      = null;
	protected String clientType     = null;
	protected String clientIP       = null;
	protected String action         = null;
	protected String agentHostname  = null;
	protected String logType        = null;
	protected String eventId        = null;

	protected AuditEventBase() {
	}
	
	protected AuditEventBase(String agentId,
							 String user,
							 Date   eventTime,
							 long   policyId,
							 String accessType,
							 short  accessResult,
							 String resultReason,
							 String aclEnforcer,
							 int repositoryType,
							 String repositoryName,
							 String sessionId,
							 String clientType,
							 String clientIP,
							 String action) {
		this.agentId        = agentId;
		this.user           = user;
		this.eventTime      = eventTime;
		this.policyId       = policyId;
		this.accessType     = accessType;
		this.accessResult   = accessResult;
		this.resultReason   = resultReason;
		this.aclEnforcer    = aclEnforcer;
		this.repositoryType = repositoryType;
		this.repositoryName = repositoryName;
		this.sessionId      = sessionId;
		this.clientType     = clientType;
		this.clientIP       = clientIP;
		this.action         = action;
	}

	/**
	 * @return the agentId
	 */
	public String getAgentId() {
		return agentId;
	}

	/**
	 * @param agentId the agentId to set
	 */
	public void setAgentId(String agentId) {
		this.agentId = agentId;
	}

	/**
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 * @param user the user to set
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * @return the timeStamp
	 */
	public Date getEventTime() {
		return eventTime;
	}

	/**
	 * @param timeStamp the timeStamp to set
	 */
	public void setEventTime(Date eventTime) {
		this.eventTime = eventTime;
	}

	/**
	 * @return the policyId
	 */
	public long getPolicyId() {
		return policyId;
	}

	/**
	 * @param policyId the policyId to set
	 */
	public void setPolicyId(long policyId) {
		this.policyId = policyId;
	}

	/**
	 * @return the accessType
	 */
	public String getAccessType() {
		return accessType;
	}

	/**
	 * @param accessType the accessType to set
	 */
	public void setAccessType(String accessType) {
		this.accessType = accessType;
	}

	/**
	 * @return the accessResult
	 */
	public short getAccessResult() {
		return accessResult;
	}

	/**
	 * @param accessResult the accessResult to set
	 */
	public void setAccessResult(short accessResult) {
		this.accessResult = accessResult;
	}

	/**
	 * @return the resultReason
	 */
	public String getResultReason() {
		return resultReason;
	}

	/**
	 * @param resultReason the resultReason to set
	 */
	public void setResultReason(String resultReason) {
		this.resultReason = resultReason;
	}

	/**
	 * @return the aclEnforcer
	 */
	public String getAclEnforcer() {
		return aclEnforcer;
	}

	/**
	 * @param aclEnforcer the aclEnforcer to set
	 */
	public void setAclEnforcer(String aclEnforcer) {
		this.aclEnforcer = aclEnforcer;
	}

	/**
	 * @return the repositoryType
	 */
	public int getRepositoryType() {
		return repositoryType;
	}

	/**
	 * @param repositoryType the repositoryType to set
	 */
	public void setRepositoryType(int repositoryType) {
		this.repositoryType = repositoryType;
	}

	/**
	 * @return the repositoryName
	 */
	public String getRepositoryName() {
		return repositoryName;
	}

	/**
	 * @param repositoryName the repositoryName to set
	 */
	public void setRepositoryName(String repositoryName) {
		this.repositoryName = repositoryName;
	}

	/**
	 * @return the sessionId
	 */
	public String getSessionId() {
		return sessionId;
	}

	/**
	 * @param sessionId the sessionId to set
	 */
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	/**
	 * @return the clientType
	 */
	public String getClientType() {
		return clientType;
	}

	/**
	 * @param clientType the clientType to set
	 */
	public void setClientType(String clientType) {
		this.clientType = clientType;
	}

	/**
	 * @return the clientIP
	 */
	public String getClientIP() {
		return clientIP;
	}

	/**
	 * @param clientIP the clientIP to set
	 */
	public void setClientIP(String clientIP) {
		this.clientIP = clientIP;
	}

	/**
	 * @return the action
	 */
	public String getAction() {
		return trim(action,MAX_ACTION_FIELD_SIZE) ;
	}

	/**
	 * @param action the action to set
	 */
	public void setAction(String action) {
		this.action = action;
	}

	public String getAgentHostname() {
		return agentHostname;
	}

	public void setAgentHostname(String agentHostname) {
		this.agentHostname = agentHostname;
	}

	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public abstract void persist(DaoManager daoManager);

	@Override
	public String toString() {
		return toString(new StringBuilder()).toString();
	}
	
	protected StringBuilder toString(StringBuilder sb) {
		sb.append("agentId=").append(agentId).append(FIELD_SEPARATOR)
		  .append("user=").append(user).append(FIELD_SEPARATOR)
		  .append("eventTime=").append(eventTime).append(FIELD_SEPARATOR)
		  .append("policyId=").append(policyId).append(FIELD_SEPARATOR)
		  .append("accessType=").append(accessType).append(FIELD_SEPARATOR)
		  .append("accessResult=").append(accessResult).append(FIELD_SEPARATOR)
		  .append("resultReason=").append(resultReason).append(FIELD_SEPARATOR)
		  .append("aclEnforcer=").append(aclEnforcer).append(FIELD_SEPARATOR)
		  .append("repositoryType=").append(repositoryType).append(FIELD_SEPARATOR)
		  .append("repositoryName=").append(repositoryName).append(FIELD_SEPARATOR)
		  .append("sessionId=").append(sessionId).append(FIELD_SEPARATOR)
		  .append("clientType=").append(clientType).append(FIELD_SEPARATOR)
		  .append("clientIP=").append(clientIP).append(FIELD_SEPARATOR)
		  .append("action=").append(action).append(FIELD_SEPARATOR)
		  .append("agentHostname=").append(agentHostname).append(FIELD_SEPARATOR)
		  .append("logType=").append(logType).append(FIELD_SEPARATOR)
		  .append("eventId=").append(eventId).append(FIELD_SEPARATOR)
		;
		return sb;
	}
	
	protected String trim(String str, int len) {
		String ret = str ;
		if (str != null) {
			if (str.length() > len) {
				ret = str.substring(0,len) ;
			}
		}
		return ret ;
	}
}
