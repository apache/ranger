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
import java.util.Properties;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.SequenceGenerator;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.MiscUtil;

/**
 * Entity implementation class for Entity: AuthzAuditEventDbObj
 *
 */
@Entity
@Table(name="xa_access_audit")
public class AuthzAuditEventDbObj implements Serializable {

	private static final Log LOG = LogFactory.getLog(AuthzAuditEventDbObj.class);

	private static final long serialVersionUID = 1L;

	static int MaxValueLengthAccessType = 255;
	static int MaxValueLengthAclEnforcer = 255;
	static int MaxValueLengthAgentId = 255;
	static int MaxValueLengthClientIp = 255;
	static int MaxValueLengthClientType = 255;
	static int MaxValueLengthRepoName = 255;
	static int MaxValueLengthResultReason = 255;
	static int MaxValueLengthSessionId = 255;
	static int MaxValueLengthRequestUser = 255;
	static int MaxValueLengthAction = 2000;
	static int MaxValueLengthRequestData = 4000;
	static int MaxValueLengthResourcePath = 4000;
	static int MaxValueLengthResourceType = 255;

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
	private long seqNum;
	private long eventCount;
	private long eventDurationMS;
	private String tags;

	public static void init(Properties props)
	{
		LOG.info("AuthzAuditEventDbObj.init()");

		final String AUDIT_DB_MAX_COLUMN_VALUE = "xasecure.audit.destination.db.max.column.length";
		MaxValueLengthAccessType = MiscUtil.getIntProperty(props, AUDIT_DB_MAX_COLUMN_VALUE + "." + "access_type", MaxValueLengthAccessType);
		logMaxColumnValue("access_type", MaxValueLengthAccessType);

		MaxValueLengthAclEnforcer = MiscUtil.getIntProperty(props, AUDIT_DB_MAX_COLUMN_VALUE + "." + "acl_enforcer", MaxValueLengthAclEnforcer);
		logMaxColumnValue("acl_enforcer", MaxValueLengthAclEnforcer);

		MaxValueLengthAction = MiscUtil.getIntProperty(props, AUDIT_DB_MAX_COLUMN_VALUE + "." + "action", MaxValueLengthAction);
		logMaxColumnValue("action", MaxValueLengthAction);

		MaxValueLengthAgentId = MiscUtil.getIntProperty(props, AUDIT_DB_MAX_COLUMN_VALUE + "." + "agent_id", MaxValueLengthAgentId);
		logMaxColumnValue("agent_id", MaxValueLengthAgentId);

		MaxValueLengthClientIp = MiscUtil.getIntProperty(props, AUDIT_DB_MAX_COLUMN_VALUE + "." + "client_id", MaxValueLengthClientIp);
		logMaxColumnValue("client_id", MaxValueLengthClientIp);

		MaxValueLengthClientType = MiscUtil.getIntProperty(props, AUDIT_DB_MAX_COLUMN_VALUE + "." + "client_type", MaxValueLengthClientType);
		logMaxColumnValue("client_type", MaxValueLengthClientType);

		MaxValueLengthRepoName = MiscUtil.getIntProperty(props, AUDIT_DB_MAX_COLUMN_VALUE + "." + "repo_name", MaxValueLengthRepoName);
		logMaxColumnValue("repo_name", MaxValueLengthRepoName);

		MaxValueLengthResultReason = MiscUtil.getIntProperty(props, AUDIT_DB_MAX_COLUMN_VALUE + "." + "result_reason", MaxValueLengthResultReason);
		logMaxColumnValue("result_reason", MaxValueLengthResultReason);

		MaxValueLengthSessionId = MiscUtil.getIntProperty(props, AUDIT_DB_MAX_COLUMN_VALUE + "." + "session_id", MaxValueLengthSessionId);
		logMaxColumnValue("session_id", MaxValueLengthSessionId);

		MaxValueLengthRequestUser = MiscUtil.getIntProperty(props, AUDIT_DB_MAX_COLUMN_VALUE + "." + "request_user", MaxValueLengthRequestUser);
		logMaxColumnValue("request_user", MaxValueLengthRequestUser);

		MaxValueLengthRequestData = MiscUtil.getIntProperty(props, AUDIT_DB_MAX_COLUMN_VALUE + "." + "request_data", MaxValueLengthRequestData);
		logMaxColumnValue("request_data", MaxValueLengthRequestData);

		MaxValueLengthResourcePath = MiscUtil.getIntProperty(props, AUDIT_DB_MAX_COLUMN_VALUE + "." + "resource_path", MaxValueLengthResourcePath);
		logMaxColumnValue("resource_path", MaxValueLengthResourcePath);

		MaxValueLengthResourceType = MiscUtil.getIntProperty(props, AUDIT_DB_MAX_COLUMN_VALUE + "." + "resource_type", MaxValueLengthResourceType);
		logMaxColumnValue("resource_type", MaxValueLengthResourceType);
	}

	public static void logMaxColumnValue(String columnName, int configuredMaxValueLength) {
		LOG.info("Setting max column value for column[" + columnName + "] to [" + configuredMaxValueLength + "].");
		if (configuredMaxValueLength == 0) {
			LOG.info("Max length of column[" + columnName + "] was 0! Column will NOT be emitted in the audit.");
		} else if (configuredMaxValueLength < 0) {
			LOG.info("Max length of column[" + columnName + "] was less than 0! Column value will never be truncated.");
		}
	}


	public AuthzAuditEventDbObj() {
		super();
	}

	public AuthzAuditEventDbObj(AuthzAuditEvent event) {
		super();
		Date utcDate=null;
		if(event.getEventTime()!=null){
			utcDate=MiscUtil.getUTCDateForLocalDate(event.getEventTime());
		}else{
			utcDate=MiscUtil.getUTCDate();
		}
		this.repositoryType = event.getRepositoryType();
		this.repositoryName = event.getRepositoryName();
		this.user           = event.getUser();
		this.timeStamp      = utcDate;
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
		this.seqNum         = event.getSeqNum();
		this.eventCount     = event.getEventCount();
		this.eventDurationMS= event.getEventDurationMS();
		this.tags           = StringUtils.join(event.getTags(), ", ");
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
		return this.repositoryType;
	}

	public void setRepositoryType(int repositoryType) {
		this.repositoryType = repositoryType;
	}

	@Column(name = "repo_name")
	public String getRepositoryName() {
		return truncate(this.repositoryName, MaxValueLengthRepoName, "repo_name");
	}

	public void setRepositoryName(String repositoryName) {
		this.repositoryName = repositoryName;
	}

	@Column(name = "request_user")
	public String getUser() {
		return truncate(this.user, MaxValueLengthRequestUser, "request_user");
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
		return truncate(this.accessType, MaxValueLengthAccessType, "access_type");
	}

	public void setAccessType(String accessType) {
		this.accessType = accessType;
	}

	@Column(name = "resource_path")
	public String getResourcePath() {
		return truncate(this.resourcePath, MaxValueLengthResourcePath, "resource_path");
	}

	public void setResourcePath(String resourcePath) {
		this.resourcePath = resourcePath;
	}

	@Column(name = "resource_type")
	public String getResourceType() {
		return truncate(this.resourceType, MaxValueLengthResourceType, "resource_type");
	}

	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}

	@Column(name = "action")
	public String getAction() {
		return truncate(this.action, MaxValueLengthAction, "action");
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
		return truncate(this.agentId, MaxValueLengthAgentId, "agent_id");
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
		return truncate(this.resultReason, MaxValueLengthResultReason, "result_reason");
	}

	public void setResultReason(String resultReason) {
		this.resultReason = resultReason;
	}

	@Column(name = "acl_enforcer")
	public String getAclEnforcer() {
		return truncate(this.aclEnforcer, MaxValueLengthAclEnforcer, "acl_enforcer");
	}

	public void setAclEnforcer(String aclEnforcer) {
		this.aclEnforcer = aclEnforcer;
	}

	@Column(name = "session_id")
	public String getSessionId() {
		return truncate(this.sessionId, MaxValueLengthSessionId, "session_id");
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	@Column(name = "client_type")
	public String getClientType() {
		return truncate(this.clientType, MaxValueLengthClientType, "client_type");
	}

	public void setClientType(String clientType) {
		this.clientType = clientType;
	}

	@Column(name = "client_ip")
	public String getClientIP() {
		return truncate(this.clientIP, MaxValueLengthClientIp, "client_ip");
	}

	public void setClientIP(String clientIP) {
		this.clientIP = clientIP;
	}

	@Column(name = "request_data")
	public String getRequestData() {
		return truncate(this.requestData, MaxValueLengthRequestData, "request_data");
	}

	public void setRequestData(String requestData) {
		this.requestData = requestData;
	}

	@Column(name = "seq_num")
	public long getSeqNum() { return this.seqNum; }

	public void setSeqNum(long seqNum) { this.seqNum = seqNum; }

	@Column(name = "event_count")
	public long getEventCount() { return this.eventCount; }

	public void setEventCount(long eventCount) { this.eventCount = eventCount; }

	@Column(name = "event_dur_ms")
	public long getEventDurationMS() { return this.eventDurationMS; }

	public void setEventDurationMS(long eventDurationMS) { this.eventDurationMS = eventDurationMS; }

	@Column(name = "tags")
	public String getTags() {
		return this.tags;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}

	static final String TruncationMarker = "...";
	static final int TruncationMarkerLength = TruncationMarker.length();

	protected String truncate(String value, int limit, String columnName) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> getTrunctedValue(%s, %d, %s)", value, limit, columnName));
		}

		String result = value;
		if (value != null) {
			if (limit < 0) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Truncation is suppressed for column[%s]: old value [%s], new value[%s]", columnName, value, result));
				}
			} else if (limit == 0) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Column[%s] is to be excluded from audit: old value [%s], new value[%s]", columnName, value, result));
				}
				result = null;
			} else {
				if (value.length() > limit) {
					if (limit <= TruncationMarkerLength) {
						// NOTE: If value is to be truncated to a size that is less than of equal to the Truncation Marker then we won't put the marker in!!
						result = value.substring(0, limit);
					} else {
						StringBuilder sb = new StringBuilder(value.substring(0, limit - TruncationMarkerLength));
						sb.append(TruncationMarker);
						result = sb.toString();
					}
					if (LOG.isDebugEnabled()) {
						LOG.debug(String.format("Truncating value for column[%s] to [%d] characters: old value [%s], new value[%s]", columnName, limit, value, result));
					}
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== getTrunctedValue(%s, %d, %s): %s", value, limit, columnName, result));
		}
		return result;
	}
}
