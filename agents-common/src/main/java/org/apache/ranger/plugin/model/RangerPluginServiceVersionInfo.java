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

package org.apache.ranger.plugin.model;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Date;

@JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RangerPluginServiceVersionInfo implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final int ENTITY_TYPE_POLICIES = 0;
	public static final int ENTITY_TYPE_TAGS     = 1;

	private Long    id         = null;
	private Date    createTime = null;

	private String serviceName;
	private String hostName;
	private String appType;
	private Integer entityType;
	private String ipAddress;
	private Long downloadedVersion;
	private Date downloadTime;
	private Long activeVersion;
	private Date activationTime;

	public RangerPluginServiceVersionInfo(Long id, Date createTime, String serviceName, String hostName, String appType, Integer entityType, String ipAddress, Long downloadedVersion, Date downloadTime, Long activeVersion, Date activationTime) {
		super();

		setId(id);
		setCreateTime(createTime);
		setServiceName(serviceName);
		setHostName(hostName);
		setAppType(appType);
		setEntityType(entityType);
		setIpAddress(ipAddress);
		setDownloadedVersion(downloadedVersion);
		setDownloadTime(downloadTime);
		setActiveVersion(activeVersion);
		setActivationTime(activationTime);
	}

	public RangerPluginServiceVersionInfo() {
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getAppType() {
		return appType;
	}

	public void setAppType(String appType) {
		this.appType = appType;
	}

	public Integer getEntityType() {
		return entityType;
	}

	public void setEntityType(Integer entityType) {
		this.entityType = entityType;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public Long getDownloadedVersion() {
		return downloadedVersion;
	}

	public void setDownloadedVersion(Long downloadedVersion) {
		this.downloadedVersion = downloadedVersion;
	}

	public Date getDownloadTime() {
		return downloadTime;
	}

	public void setDownloadTime(Date downloadTime) {
		this.downloadTime = downloadTime;
	}

	public Long getActiveVersion() {
		return activeVersion;
	}

	public void setActiveVersion(Long activeVersion) {
		this.activeVersion = activeVersion;
	}

	public Date getActivationTime() {
		return activationTime;
	}

	public void setActivationTime(Date activationTime) {
		this.activationTime = activationTime;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerPluginServiceVersionInfo={");

		sb.append("id={").append(id).append("} ");
		sb.append("createTime={").append(createTime).append("} ");
		sb.append("serviceName={").append(serviceName).append("} ");
		sb.append("hostName={").append(hostName).append("} ");
		sb.append("appType={").append(appType).append("} ");
		sb.append("entityType={").append(entityType).append("} ");
		sb.append("ipAddress={").append(ipAddress).append("} ");
		sb.append("downloadedVersion={").append(downloadedVersion).append("} ");
		sb.append("downloadTime={").append(downloadTime).append("} ");
		sb.append("activeVersion={").append(activeVersion).append("} ");
		sb.append("activationTime={").append(activationTime).append("} ");

		sb.append(" }");

		return sb;
	}
}

