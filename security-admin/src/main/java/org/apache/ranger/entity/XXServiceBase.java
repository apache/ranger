/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.entity;

import java.util.Date;
import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Version;

@MappedSuperclass
public abstract class XXServiceBase extends XXDBBase {
	private static final long serialVersionUID = 1L;

	/**
	 * Global Id for the object
	 * <ul>
	 * <li>The maximum length for this attribute is <b>512</b>.
	 * </ul>
	 *
	 */
	@Column(name = "guid", unique = true, nullable = false, length = 512)
	protected String guid;

	/**
	 * version of the XXServiceDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Version
	@Column(name = "version")
	protected Long version;

	/**
	 * type of the XXService
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "type")
	protected Long type;

	/**
	 * name of the XXService
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "name")
	protected String name;

	/**
	 * displayName of the XXService
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "display_name")
	protected String displayName;
	/**
	 * tagService of the XXService
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "tag_service")
	protected Long tagService;

	/**
	 * policyVersion of the XXService
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "policy_version")
	protected Long policyVersion;

	/**
	 * policyUpdateTime of the XXService
	 * <ul>
	 * </ul>
	 *
	 */
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "policy_update_time")
	protected Date policyUpdateTime;

	/**
	 * tagVersion of the XXService
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "tag_version")
	protected Long tagVersion;

	/**
	 * tagUpdateTime of the XXService
	 * <ul>
	 * </ul>
	 *
	 */
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "tag_update_time")
	protected Date tagUpdateTime;

	/**
	 * description of the XXService
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "description")
	protected String description;

	/**
	 * isEnabled of the XXService
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "is_enabled")
	protected Boolean isEnabled;

	/**
	 * @return the gUID
	 */
	public String getGuid() {
		return this.guid;
	}

	/**
	 * @param guid
	 *            the gUID to set
	 */
	public void setGuid(String guid) {
		this.guid = guid;
	}

	/**
	 * This method sets the value to the member attribute <b> version</b> . You
	 * cannot set null to the attribute.
	 *
	 * @param version
	 *            Value to set member attribute <b> version</b>
	 */
	public void setVersion(Long version) {
		this.version = version;
	}

	/**
	 * Returns the value for the member attribute <b>version</b>
	 *
	 * @return Date - value of member attribute <b>version</b> .
	 */
	public Long getVersion() {
		return this.version;
	}

	/**
	 * This method sets the value to the member attribute <b> type</b> . You
	 * cannot set null to the attribute.
	 *
	 * @param type
	 *            Value to set member attribute <b> type</b>
	 */
	public void setType(Long type) {
		this.type = type;
	}

	/**
	 * Returns the value for the member attribute <b>type</b>
	 *
	 * @return Date - value of member attribute <b>type</b> .
	 */
	public Long getType() {
		return this.type;
	}

	/**
	 * This method sets the value to the member attribute <b> name</b> . You
	 * cannot set null to the attribute.
	 *
	 * @param name
	 *            Value to set member attribute <b> name</b>
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Returns the value for the member attribute <b>name</b>
	 *
	 * @return Date - value of member attribute <b>name</b> .
	 */
	public String getName() {
		return this.name;
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	/**
	 * This method sets the value to the member attribute <b> tagService</b> .
	 *
	 * @param tagService
	 *            Value to set member attribute <b> tagService</b>
	 */
	public void setTagService(Long tagService) {
		this.tagService = tagService;
	}

	/**
	 * Returns the value for the member attribute <b>tagService</b>
	 *
	 * @return Long - value of member attribute <b>tagService</b> .
	 */
	public Long getTagService() {
		return this.tagService;
	}

	/**
	 * This method sets the value to the member attribute <b> policyVersion</b>
	 * . You cannot set null to the attribute.
	 *
	 * @param policyVersion
	 *            Value to set member attribute <b> policyVersion</b>
	 */
	public void setPolicyVersion(Long policyVersion) {
		this.policyVersion = policyVersion;
	}

	/**
	 * Returns the value for the member attribute <b>policyVersion</b>
	 *
	 * @return Date - value of member attribute <b>policyVersion</b> .
	 */
	public Long getPolicyVersion() {
		return this.policyVersion;
	}

	/**
	 * This method sets the value to the member attribute <b>
	 * policyUpdateTime</b> . You cannot set null to the attribute.
	 *
	 * @param policyUpdateTime
	 *            Value to set member attribute <b> policyUpdateTime</b>
	 */
	public void setPolicyUpdateTime(Date policyUpdateTime) {
		this.policyUpdateTime = policyUpdateTime;
	}

	/**
	 * Returns the value for the member attribute <b>policyUpdateTime</b>
	 *
	 * @return Date - value of member attribute <b>policyUpdateTime</b> .
	 */
	public Date getPolicyUpdateTime() {
		return this.policyUpdateTime;
	}

	/**
	 * This method sets the value to the member attribute <b> tagVersion</b>
	 * . You cannot set null to the attribute.
	 *
	 * @param tagVersion
	 *            Value to set member attribute <b> tagVersion</b>
	 */
	public void setTagVersion(Long tagVersion) {
		this.tagVersion = tagVersion;
	}

	/**
	 * Returns the value for the member attribute <b>tagVersion</b>
	 *
	 * @return Long - value of member attribute <b>tagVersion</b> .
	 */
	public Long getTagVersion() {
		return this.tagVersion;
	}

	/**
	 * This method sets the value to the member attribute <b>
	 * tagUpdateTime</b> . You cannot set null to the attribute.
	 *
	 * @param tagUpdateTime
	 *            Value to set member attribute <b> tagUpdateTime</b>
	 */
	public void setTagUpdateTime(Date tagUpdateTime) {
		this.tagUpdateTime = tagUpdateTime;
	}

	/**
	 * Returns the value for the member attribute <b>tagUpdateTime</b>
	 *
	 * @return Date - value of member attribute <b>tagUpdateTime</b> .
	 */
	public Date getTagUpdateTime() {
		return this.tagUpdateTime;
	}

	/**
	 * This method sets the value to the member attribute <b> description</b> .
	 * You cannot set null to the attribute.
	 *
	 * @param description
	 *            Value to set member attribute <b> description</b>
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * Returns the value for the member attribute <b>description</b>
	 *
	 * @return Date - value of member attribute <b>description</b> .
	 */
	public String getDescription() {
		return this.description;
	}

	/**
	 * This method sets the value to the member attribute <b> isEnabled</b> .
	 * You cannot set null to the attribute.
	 *
	 * @param isEnabled
	 *            Value to set member attribute <b> isEnabled</b>
	 */
	public void setIsEnabled(Boolean isEnabled) {
		this.isEnabled = isEnabled;
	}

	/**
	 * Returns the value for the member attribute <b>isEnabled</b>
	 *
	 * @return Date - value of member attribute <b>isEnabled</b> .
	 */
	public Boolean getIsenabled() {
		return this.isEnabled;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		final boolean ret;

		if (this == obj) {
			ret = true;
		} else if (obj == null || getClass() != obj.getClass()) {
			ret = false;
		} else {
			XXServiceBase other = (XXServiceBase) obj;

			ret = super.equals(obj) &&
				  Objects.equals(description, other.description) &&
				  Objects.equals(isEnabled, other.isEnabled) &&
				  Objects.equals(name, other.name) &&
				  Objects.equals(policyUpdateTime, other.policyUpdateTime) &&
				  Objects.equals(policyVersion, other.policyVersion) &&
				  Objects.equals(tagService, other.tagService) &&
				  Objects.equals(tagVersion, other.tagVersion) &&
				  Objects.equals(tagUpdateTime, other.tagUpdateTime) &&
				  Objects.equals(type, other.type) &&
				  Objects.equals(version, other.version) &&
				  Objects.equals(guid, other.guid);
		}

		return ret;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "XXServiceBase [" + super.toString() + " guid=" + guid + ", version=" + version + ", type=" + type
				+ ", name=" + name +", displayName=" + displayName + ", tagService=" + tagService + ", policyVersion=" + policyVersion + ", policyUpdateTime=" + policyUpdateTime
				+ ", tagVersion=" + tagVersion + ", tagUpdateTime=" + tagUpdateTime
				+ ", description=" + description + ", isEnabled=" + isEnabled + "]";
	}

}
