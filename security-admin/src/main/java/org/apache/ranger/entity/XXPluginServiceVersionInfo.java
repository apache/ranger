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

package org.apache.ranger.entity;

import java.util.Date;

import javax.persistence.Cacheable;
import javax.persistence.Entity;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.DateUtil;

@Entity
@Cacheable
@XmlRootElement
@Table(name = "x_plugin_service_version_info")
public class XXPluginServiceVersionInfo implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@SequenceGenerator(name = "X_PLUGIN_SERVICE_VERSION_INFO_SEQ", sequenceName = "X_PLUGIN_SERVICE_VERSION_INFO_SEQ", allocationSize = 1)
	@GeneratedValue(strategy = GenerationType.AUTO, generator = "X_PLUGIN_SERVICE_VERSION_INFO_SEQ")
	@Column(name = "id")
	protected Long id;

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name="CREATE_TIME"   )
	protected Date createTime = DateUtil.getUTCDate();

	@Column(name = "service_name")
	protected String serviceName;

	@Column(name = "host_name")
	protected String hostName;

	@Column(name = "app_type")
	protected String appType;

	@Column(name = "entity_type")
	protected Integer entityType;

	@Column(name = "ip_address")
	protected String ipAddress;

	@Column(name = "downloaded_version")
	protected Long downloadedVersion;

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name="download_time"   )
	protected Date downloadTime = DateUtil.getUTCDate();

	@Column(name = "active_version")
	protected Long activeVersion;

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name="activation_time"   )
	protected Date activationTime = DateUtil.getUTCDate();

	/**
	 * Default constructor. This will set all the attributes to default value.
	 */
	public XXPluginServiceVersionInfo ( ) {
	}

	public int getMyClassType( ) {
	    return AppConstants.CLASS_TYPE_NONE;
	}

	public String getMyDisplayValue() {
		return null;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getId() {
		return this.id;
	}

	public void setCreateTime( Date createTime ) {
		this.createTime = createTime;
	}

	public Date getCreateTime( ) {
		return this.createTime;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getServiceName() {
		return this.serviceName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getHostName() {
		return this.hostName;
	}

	public void setAppType(String appType) {
		this.appType = appType;
	}

	public String getAppType() {
		return this.appType;
	}

	public void setEntityType(Integer entityType) {
		this.entityType = entityType;
	}

	public Integer getEntityType() {
		return this.entityType;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public String getIpAddress() {
		return this.ipAddress;
	}

	public void setDownloadedVersion(Long downloadedVersion) {
		this.downloadedVersion = downloadedVersion;
	}

	public Long getDownloadedVersion() {
		return this.downloadedVersion;
	}

	public void setDownloadTime( Date downloadTime ) {
		this.downloadTime = downloadTime;
	}

	public Date getDownloadTime( ) {
		return this.downloadTime;
	}

	public void setActiveVersion(Long activeVersion) {
		this.activeVersion = activeVersion;
	}

	public Long getActiveVersion() {
		return this.activeVersion;
	}

	public void setActivationTime( Date activationTime ) {
		this.activationTime = activationTime;
	}

	public Date getActivationTime( ) {
		return this.activationTime;
	}


	/**
	 * This return the bean content in string format
	 * @return formatedStr
	*/
	@Override
	public String toString( ) {
		String str = "XXPluginServiceVersionInfo={";
		str += "id={" + id + "} ";
		str += "createTime={" + createTime + "} ";
		str += "serviceName={" + serviceName + "} ";
		str += "hostName={" + hostName + "} ";
		str += "appType={" + appType + "} ";
		str += "entityType={" + entityType + "} ";
		str += "ipAddress={" + ipAddress + "} ";
		str += "downloadedVersion={" + downloadedVersion + "} ";
		str += "downloadTime={" + downloadTime + "} ";
		str += "activeVersion={" + activeVersion + "} ";
		str += "activationTime={" + activationTime + "} ";
		str += "}";
		return str;
	}

	/**
	 * Checks for all attributes except referenced db objects
	 * @return true if all attributes match
	*/
	@Override
	public boolean equals( Object obj) {
		if (obj == null)
			return false;
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		XXPluginServiceVersionInfo other = (XXPluginServiceVersionInfo) obj;
		if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
			return false;
		}
		if ((this.createTime == null && other.createTime != null) || (this.createTime != null && !this.createTime.equals(other.createTime))) {
			return false;
		}
		if ((this.serviceName == null && other.serviceName != null) || (this.serviceName != null && !this.serviceName.equals(other.serviceName))) {
			return false;
		}
		if ((this.hostName == null && other.hostName != null) || (this.hostName != null && !this.hostName.equals(other.hostName))) {
			return false;
		}
		if ((this.appType == null && other.appType != null) || (this.appType != null && !this.appType.equals(other.appType))) {
			return false;
		}
		if ((this.entityType == null && other.entityType != null) || (this.entityType != null && !this.entityType.equals(other.entityType))) {
			return false;
		}
		if ((this.ipAddress == null && other.ipAddress != null) || (this.ipAddress != null && !this.ipAddress.equals(other.ipAddress))) {
			return false;
		}
		if ((this.downloadedVersion == null && other.downloadedVersion != null) || (this.downloadedVersion != null && !this.downloadedVersion.equals(other.downloadedVersion))) {
			return false;
		}
		if ((this.downloadTime == null && other.downloadTime != null) || (this.downloadTime != null && !this.downloadTime.equals(other.downloadTime))) {
			return false;
		}
		if ((this.activeVersion == null && other.activeVersion != null) || (this.activeVersion != null && !this.activeVersion.equals(other.activeVersion))) {
			return false;
		}
		if ((this.activationTime == null && other.activationTime != null) || (this.activationTime != null && !this.activationTime.equals(other.activationTime))) {
			return false;
		}
		return true;
	}

	public static boolean equals(Object object1, Object object2) {
		if (object1 == object2) {
			return true;
		}
		if ((object1 == null) || (object2 == null)) {
			return false;
		}
		return object1.equals(object2);
	}

}
