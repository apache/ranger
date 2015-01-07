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

 package org.apache.ranger.audit.entity;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.SequenceGenerator;

import org.apache.ranger.audit.model.EnumRepositoryType;
import org.apache.ranger.audit.model.AuthzAuditEvent;

/**
 * Entity implementation class for Entity: AuthzAuditEventDbObj
 *
 */
@Entity
@Table(name="xa_access_audit")
public class AuthzAuditEventDbObj implements Serializable {
	private static final long serialVersionUID = 1L;

	private long   auditId;
	private int    repositoryType;
	private String repositoryName;
	private String user;
	private Date   timeStamp;
	private String accessType;
	private String resourcePath;
	private String resourceType;
	private String action;
	private int    accessResult;
	private String agentId;
	private long   policyId;
	private String resultReason;
	private String aclEnforcer;
	private String sessionId;
	private String clientType;
	private String clientIP;
	private String requestData;


	public AuthzAuditEventDbObj() {
		super();
	}

	public AuthzAuditEventDbObj(AuthzAuditEvent event) {
		super();

		this.repositoryType = event.getRepositoryType();
		this.repositoryName = event.getRepositoryName();
		this.user           = event.getUser();
		this.timeStamp      = event.getEventTime();
		this.accessType     = event.getAccessType();
		this.resourcePath   = event.getResourcePath();
		this.resourceType   = event.getResourceType();
		this.action         = event.getAction();
		this.accessResult   = event.getAccessResult();
		this.agentId        = event.getAgentId();
		this.policyId       = event.getPolicyId();
		this.resultReason   = event.getResultReason();
		this.aclEnforcer    = event.getAclEnforcer();
		this.sessionId      = event.getSessionId();
		this.clientType     = event.getClientType();
		this.clientIP       = event.getClientIP();
		this.requestData    = event.getRequestData();
	}

	@Id
	@SequenceGenerator(name="XA_ACCESS_AUDIT_SEQ",sequenceName="XA_ACCESS_AUDIT_SEQ",allocationSize=1)
	@GeneratedValue(strategy=GenerationType.AUTO,generator="XA_ACCESS_AUDIT_SEQ")
	@Column(name = "id", unique = true, nullable = false)
	public long getAuditId() {
		return this.auditId;
	}

	public void setAuditId(long auditId) {
		this.auditId = auditId;
	}

	@Column(name = "repo_type")
	public int getRepositoryType() {
		return this.repositoryType ;
	}

	public void setRepositoryType(int repositoryType) {
		this.repositoryType = repositoryType;
	}

	@Column(name = "repo_name")
	public String getRepositoryName() {
		return this.repositoryName;
	}

	public void setRepositoryName(String repositoryName) {
		this.repositoryName = repositoryName;
	}

	@Column(name = "request_user")
	public String getUser() {
		return this.user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "event_time")
	public Date getTimeStamp() {
		return this.timeStamp;
	}

	public void setTimeStamp(Date timeStamp) {
		this.timeStamp = timeStamp;
	}

	@Column(name = "access_type")
	public String getAccessType() {
		return this.accessType;
	}

	public void setAccessType(String accessType) {
		this.accessType = accessType;
	}

	@Column(name = "resource_path")
	public String getResourcePath() {
		return this.resourcePath;
	}

	public void setResourcePath(String resourcePath) {
		this.resourcePath = resourcePath;
	}

	@Column(name = "resource_type")
	public String getResourceType() {
		return this.resourceType;
	}

	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}

	@Column(name = "action")
	public String getAction() {
		return this.action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	@Column(name = "access_result")
	public int getAccessResult() {
		return this.accessResult;
	}

	public void setAccessResult(int accessResult) {
		this.accessResult = accessResult;
	}

	@Column(name = "agent_id")
	public String getAgentId() {
		return agentId;
	}

	public void setAgentId(String agentId) {
		this.agentId = agentId;
	}

	@Column(name = "policy_id")
	public long getPolicyId() {
		return this.policyId;
	}

	public void setPolicyId(long policyId) {
		this.policyId = policyId;
	}

	@Column(name = "result_reason")
	public String getResultReason() {
		return this.resultReason;
	}

	public void setResultReason(String resultReason) {
		this.resultReason = resultReason;
	}

	@Column(name = "acl_enforcer")
	public String getAclEnforcer() {
		return this.aclEnforcer;
	}

	public void setAclEnforcer(String aclEnforcer) {
		this.aclEnforcer = aclEnforcer;
	}

	@Column(name = "session_id")
	public String getSessionId() {
		return this.sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	@Column(name = "client_type")
	public String getClientType() {
		return this.clientType;
	}

	public void setClientType(String clientType) {
		this.clientType = clientType;
	}

	@Column(name = "client_ip")
	public String getClientIP() {
		return this.clientIP;
	}

	public void setClientIP(String clientIP) {
		this.clientIP = clientIP;
	}

	@Column(name = "request_data")
	public String getRequestData() {
		return this.requestData;
	}

	public void setRequestData(String requestData) {
		this.requestData = requestData;
	}
}
