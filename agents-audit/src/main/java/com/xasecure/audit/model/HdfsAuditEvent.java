package com.xasecure.audit.model;

import java.util.Date;

import com.xasecure.audit.provider.AuditProvider;

public class HdfsAuditEvent extends AuditEventBase {
	protected String resourcePath;
	protected String resourceType;

	public HdfsAuditEvent() {
	}

	public HdfsAuditEvent(String agentId,
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
						  String resourcePath,
						  String resourceType,
						  String action) {
		super(agentId, user, eventTime, policyId, accessType, accessResult, resultReason, aclEnforcer, repositoryType, repositoryName, sessionId, clientType, clientIP, action);
		
		this.resourcePath = resourcePath;
		this.resourceType = resourceType;
	}

	/**
	 * @return the resourcePath
	 */
	public String getResourcePath() {
		return resourcePath;
	}

	/**
	 * @param resourcePath the resourcePath to set
	 */
	public void setResourcePath(String resourcePath) {
		this.resourcePath = resourcePath;
	}

	/**
	 * @return the resourceType
	 */
	public String getResourceType() {
		return resourceType;
	}

	/**
	 * @param resourceType the resourceType to set
	 */
	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}

	@Override
	public void logEvent(AuditProvider provider) {
		provider.log(this);
	}

	@Override
	protected StringBuilder toString(StringBuilder sb) {
		sb.append("HdfsAuditEvent{");
		
		super.toString(sb)
		     .append("resourcePath=").append(resourcePath).append(FIELD_SEPARATOR)
		     .append("resourceType=").append(resourceType).append(FIELD_SEPARATOR);
		
		sb.append("}");
		
		return sb;
	}
}
