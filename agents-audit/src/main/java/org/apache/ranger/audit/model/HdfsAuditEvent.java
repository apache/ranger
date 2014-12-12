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

 package org.apache.ranger.audit.model;

import java.util.Date;

import org.apache.ranger.audit.dao.DaoManager;
import org.apache.ranger.audit.entity.XXHdfsAuditEvent;

import com.google.gson.annotations.SerializedName;  

public class HdfsAuditEvent extends AuditEventBase {
	@SerializedName("resource")  
	protected String resourcePath;

	@SerializedName("resType")  
	protected String resourceType;

	public HdfsAuditEvent() {
		this.repositoryType = EnumRepositoryType.HDFS;
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
	public void persist(DaoManager daoManager) {
		daoManager.getXAHdfsAuditEventDao().create(new XXHdfsAuditEvent(this));
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
