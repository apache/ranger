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

 package org.apache.ranger.entity.view;


import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.RangerConstants;

import javax.persistence.*;
import java.util.Date;

@Entity
@Table(name="vx_principal")
public class VXXPrincipal implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="PRINCIPAL_NAME", nullable=false)
	protected String principalName;

	@Id
	@Column(name="PRINCIPAL_TYPE", nullable=false)
	protected Integer principalType;

	@Column(name="STATUS", nullable=false)
	protected int status = RangerConstants.STATUS_DISABLED;

	@Column(name="IS_VISIBLE", nullable=false )
	protected Integer isVisible;

	@Column(name="OTHER_ATTRIBUTES")
	protected String otherAttributes;

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name="CREATE_TIME"  , nullable=false )
	protected Date createTime = DateUtil.getUTCDate();

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name="UPDATE_TIME"  , nullable=false )
	protected Date updateTime = DateUtil.getUTCDate();

	@Column(name="ADDED_BY_ID"   )
	protected Long addedByUserId;

	@Column(name="UPD_BY_ID"   )
	protected Long updatedByUserId;



	/**
	 * @return the principalName
	 */
	public String getPrincipalName() {
		return principalName;
	}

	/**
	 * @param principalName the principalName to set
	 */
	public void setPrincipalName(String principalName) {
		this.principalName = principalName;
	}

	/**
	 * @return the principalType
	 */
	public Integer getPrincipalType() {
		return principalType;
	}

	/**
	 * @param principalType the principalType to set
	 */
	public void setPrincipalType(Integer principalType) {
		this.principalType = principalType;
	}

	/**
	 * This method sets the value to the member attribute <b>status</b>.
	 * You cannot set null to the attribute.
	 * @param status Value to set member attribute <b>status</b>
	 */
	public void setStatus( int status ) {
		this.status = status;
	}

	/**
	 * Returns the value for the member attribute <b>status</b>
	 * @return int - value of member attribute <b>status</b>.
	 */
	public int getStatus( ) {
		return this.status;
	}

	/**
	 * This method sets the value to the member attribute <b>isVisible</b>.
	 * You cannot set null to the attribute.
	 * @param isVisible Value to set member attribute <b>isVisible</b>
	 */
	public void setIsVisible(Integer isVisible) {
		this.isVisible = isVisible;
	}

	/**
	 * Returns the value for the member attribute <b>isVisible</b>
	 * @return int - value of member attribute <b>isVisible</b>.
	 */
	public Integer getIsVisible() {
		return isVisible;
	}

	/**
	 * This method sets JSON {@link String} representation of additional store attributes.
	 * This method accepts null values.
	 * @param otherAttributes
	 */
	public void setOtherAttributes(String otherAttributes) {
		this.otherAttributes = otherAttributes;
	}

	/**
	 * @return JSON {@link String} representation of additional store attributes if available,
	 * <code>null</code> otherwise.
	 */
	public String getOtherAttributes() {
		return otherAttributes;
	}

	/**
	 * @return the createTime
	 */
	public Date getCreateTime() {
		return createTime;
	}

	/**
	 * @param createTime the createTime to set
	 */
	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	/**
	 * @return the updateTime
	 */
	public Date getUpdateTime() {
		return updateTime;
	}

	/**
	 * @param updateTime the updateTime to set
	 */
	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}

	/**
	 * @return the addedByUserId
	 */
	public Long getAddedByUserId() {
		return addedByUserId;
	}

	/**
	 * @param addedByUserId the addedByUserId to set
	 */
	public void setAddedByUserId(Long addedByUserId) {
		this.addedByUserId = addedByUserId;
	}


	/**
	 * @return the updatedByUserId
	 */
	public Long getUpdatedByUserId() {
		return updatedByUserId;
	}

	/**
	 * @param updatedByUserId the updatedByUserId to set
	 */
	public void setUpdatedByUserId(Long updatedByUserId) {
		this.updatedByUserId = updatedByUserId;
	}
}
