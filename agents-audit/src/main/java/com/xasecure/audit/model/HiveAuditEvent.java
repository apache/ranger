package com.xasecure.audit.model;

import java.util.Date;

import com.xasecure.audit.dao.DaoManager;
import com.xasecure.audit.entity.XXHiveAuditEvent;

public class HiveAuditEvent extends AuditEventBase {
	protected String resourcePath;
	protected String resourceType;
	protected String requestData;

	public HiveAuditEvent() {
		this.repositoryType = EnumRepositoryType.HIVE;
	}

	public HiveAuditEvent(String agentId,
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
						  String requestData,
						  String action) {
		super(agentId, user, eventTime, policyId, accessType, accessResult, resultReason, aclEnforcer, repositoryType, repositoryName, sessionId, clientType, clientIP, action);
		
		this.resourcePath = resourcePath;
		this.resourceType = resourceType;
		this.requestData  = requestData;
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

	/**
	 * @return the requestData
	 */
	public String getRequestData() {
		return trim(requestData,MAX_REQUEST_DATA_FIELD_SIZE);
	}

	/**
	 * @param requestData the requestData to set
	 */
	public void setRequestData(String requestData) {
		this.requestData = requestData;
	}

	@Override
	public void persist(DaoManager daoManager) {
		daoManager.getXAHiveAuditEvent().create(new XXHiveAuditEvent(this));
	}

	@Override
	protected StringBuilder toString(StringBuilder sb) {
		sb.append("HiveAuditEvent{");
		
		super.toString(sb)
		     .append("resourcePath=").append(resourcePath).append(FIELD_SEPARATOR)
		     .append("resourceType=").append(resourceType).append(FIELD_SEPARATOR)
		     .append("requestData=").append(requestData).append(FIELD_SEPARATOR);
		
		sb.append("}");
		
		return sb;
	}
}
