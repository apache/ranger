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

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RangerPluginInfo implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final int ENTITY_TYPE_POLICIES = 0;
	public static final int ENTITY_TYPE_TAGS     = 1;

	public static final String PLUGIN_INFO_POLICY_DOWNLOAD_TIME      = "policyDownloadTime";
	public static final String PLUGIN_INFO_POLICY_DOWNLOADED_VERSION = "policyDownloadedVersion";
	public static final String PLUGIN_INFO_POLICY_ACTIVATION_TIME    = "policyActivationTime";
	public static final String PLUGIN_INFO_POLICY_ACTIVE_VERSION     = "policyActiveVersion";
	public static final String PLUGIN_INFO_TAG_DOWNLOAD_TIME         = "tagDownloadTime";
	public static final String PLUGIN_INFO_TAG_DOWNLOADED_VERSION    = "tagDownloadedVersion";
	public static final String PLUGIN_INFO_TAG_ACTIVATION_TIME       = "tagActivationTime";
	public static final String PLUGIN_INFO_TAG_ACTIVE_VERSION        = "tagActiveVersion";


	public static final String RANGER_ADMIN_LAST_POLICY_UPDATE_TIME  = "lastPolicyUpdateTime";
	public static final String RANGER_ADMIN_LATEST_POLICY_VERSION    = "latestPolicyVersion";
	public static final String RANGER_ADMIN_LAST_TAG_UPDATE_TIME     = "lastTagUpdateTime";
	public static final String RANGER_ADMIN_LATEST_TAG_VERSION       = "latestTagVersion";

	private Long    id;
	private Date    createTime;
	private Date    updateTime;

	private String serviceName;
	private String hostName;
	private String appType;
	private String ipAddress;
	private Map<String, String> info;

	public RangerPluginInfo(Long id, Date createTime, Date updateTime, String serviceName, String appType, String hostName, String ipAddress, Map<String, String> info) {
		super();

		setId(id);
		setCreateTime(createTime);
		setUpdateTime(updateTime);
		setServiceName(serviceName);
		setAppType(appType);
		setHostName(hostName);
		setIpAddress(ipAddress);
		setInfo(info);
	}

	public RangerPluginInfo() {
		this(null, null, null, null, null, null, null, null);
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

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
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

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public Map<String, String> getInfo() {
		return info;
	}

	public void setInfo(Map<String, String> info) {
		this.info = info == null ? new HashMap<String, String>() : info;
	}

	@JsonIgnore
	public void setPolicyDownloadTime(Long policyDownloadTime) {
		getInfo().put(PLUGIN_INFO_POLICY_DOWNLOAD_TIME, policyDownloadTime == null ? null : Long.toString(policyDownloadTime));
	}

	@JsonIgnore
	public Long getPolicyDownloadTime() {
		String downloadTimeString = getInfo().get(PLUGIN_INFO_POLICY_DOWNLOAD_TIME);
		return StringUtils.isNotBlank(downloadTimeString) ? Long.valueOf(downloadTimeString) : null;
	}

	@JsonIgnore
	public void setPolicyDownloadedVersion(Long policyDownloadedVersion) {
		getInfo().put(PLUGIN_INFO_POLICY_DOWNLOADED_VERSION, policyDownloadedVersion == null ? null : Long.toString(policyDownloadedVersion));
	}

	@JsonIgnore
	public Long getPolicyDownloadedVersion() {
		String downloadedVersionString = getInfo().get(PLUGIN_INFO_POLICY_DOWNLOADED_VERSION);
		return StringUtils.isNotBlank(downloadedVersionString) ? Long.valueOf(downloadedVersionString) : null;
	}

	@JsonIgnore
	public void setPolicyActivationTime(Long policyActivationTime) {
		getInfo().put(PLUGIN_INFO_POLICY_ACTIVATION_TIME, policyActivationTime == null ? null : Long.toString(policyActivationTime));
	}

	@JsonIgnore
	public Long getPolicyActivationTime() {
		String activationTimeString = getInfo().get(PLUGIN_INFO_POLICY_ACTIVATION_TIME);
		return StringUtils.isNotBlank(activationTimeString) ? Long.valueOf(activationTimeString) : null;
	}

	@JsonIgnore
	public void setPolicyActiveVersion(Long policyActiveVersion) {
		getInfo().put(PLUGIN_INFO_POLICY_ACTIVE_VERSION, policyActiveVersion == null ? null : Long.toString(policyActiveVersion));
	}

	@JsonIgnore
	public Long getPolicyActiveVersion() {
		String activeVersionString = getInfo().get(PLUGIN_INFO_POLICY_ACTIVE_VERSION);
		return StringUtils.isNotBlank(activeVersionString) ? Long.valueOf(activeVersionString) : null;
	}

	@JsonIgnore
	public void setTagDownloadTime(Long tagDownloadTime) {
		getInfo().put(PLUGIN_INFO_TAG_DOWNLOAD_TIME, tagDownloadTime == null ? null : Long.toString(tagDownloadTime));
	}

	@JsonIgnore
	public Long getTagDownloadTime() {
		String downloadTimeString = getInfo().get(PLUGIN_INFO_TAG_DOWNLOAD_TIME);
		return StringUtils.isNotBlank(downloadTimeString) ? Long.valueOf(downloadTimeString) : null;
	}

	@JsonIgnore
	public void setTagDownloadedVersion(Long tagDownloadedVersion) {
		getInfo().put(PLUGIN_INFO_TAG_DOWNLOADED_VERSION, tagDownloadedVersion == null ? null : Long.toString(tagDownloadedVersion));
	}

	@JsonIgnore
	public Long getTagDownloadedVersion() {
		String downloadedVersion = getInfo().get(PLUGIN_INFO_TAG_DOWNLOADED_VERSION);
		return StringUtils.isNotBlank(downloadedVersion) ? Long.valueOf(downloadedVersion) : null;
	}

	@JsonIgnore
	public void setTagActivationTime(Long tagActivationTime) {
		getInfo().put(PLUGIN_INFO_TAG_ACTIVATION_TIME, tagActivationTime == null ? null : Long.toString(tagActivationTime));
	}

	@JsonIgnore
	public Long getTagActivationTime() {
		String activationTimeString = getInfo().get(PLUGIN_INFO_TAG_ACTIVATION_TIME);
		return StringUtils.isNotBlank(activationTimeString) ? Long.valueOf(activationTimeString) : null;
	}

	@JsonIgnore
	public void setTagActiveVersion(Long tagActiveVersion) {
		getInfo().put(PLUGIN_INFO_TAG_ACTIVE_VERSION, tagActiveVersion == null ? null : Long.toString(tagActiveVersion));
	}

	@JsonIgnore
	public Long getTagActiveVersion() {
		String activeVersionString = getInfo().get(PLUGIN_INFO_TAG_ACTIVE_VERSION);
		return StringUtils.isNotBlank(activeVersionString) ? Long.valueOf(activeVersionString) : null;
	}

	@JsonIgnore
	public Long getLatestPolicyVersion() {
		String latestPolicyVersionString = getInfo().get(RANGER_ADMIN_LATEST_POLICY_VERSION);
		return StringUtils.isNotBlank(latestPolicyVersionString) ? Long.valueOf(latestPolicyVersionString) : null;
	}

	@JsonIgnore
	public Long getLastPolicyUpdateTime() {
		String updateTimeString = getInfo().get(RANGER_ADMIN_LAST_POLICY_UPDATE_TIME);
		return StringUtils.isNotBlank(updateTimeString) ? Long.valueOf(updateTimeString) : null;
	}

	@JsonIgnore
	public Long getLatestTagVersion() {
		String latestTagVersionString = getInfo().get(RANGER_ADMIN_LATEST_TAG_VERSION);
		return StringUtils.isNotBlank(latestTagVersionString) ? Long.valueOf(latestTagVersionString) : null;
	}

	@JsonIgnore
	public Long getLastTagUpdateTime() {
		String updateTimeString = getInfo().get(RANGER_ADMIN_LAST_TAG_UPDATE_TIME);
		return StringUtils.isNotBlank(updateTimeString) ? Long.valueOf(updateTimeString) : null;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerPluginInfo={");

		sb.append("id={").append(id).append("} ");
		sb.append("createTime={").append(createTime).append("} ");
		sb.append("updateTime={").append(updateTime).append("} ");
		sb.append("serviceName={").append(serviceName).append("} ");
		sb.append("hostName={").append(hostName).append("} ");
		sb.append("appType={").append(appType).append("} ");
		sb.append("ipAddress={").append(ipAddress).append("} ");
		sb.append("info={").append(info).append("} ");

		sb.append(" }");

		return sb;
	}
}

