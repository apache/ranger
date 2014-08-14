package com.xasecure.entity;
/*
 * Copyright (c) 2014 XASecure
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure
 */

/**
 * Base JPA class with id, versionNumber and other common attributes
 * 
 */

import java.util.*;
import javax.persistence.*;
import javax.xml.bind.annotation.*;
import com.xasecure.common.*;
import com.xasecure.entity.*;


@MappedSuperclass @EntityListeners( com.xasecure.common.db.JPABeanCallbacks.class)
@XmlRootElement
public class XXDBBase extends Object implements java.io.Serializable {
	private static final long serialVersionUID = 1L;



	/**
	 * Id for the object
	 * <ul>
	 * <li>This attribute is the <b>Primary Key</b> for this class<br>.
	 * </ul>
	 *
	 */
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	@Column(name="ID"   )
	protected Long id;

	/**
	 * Date/Time creation of this user.
	 * <ul>
	 * </ul>
	 *
	 */
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name="CREATE_TIME"   )
	protected Date createTime = DateUtil.getUTCDate();

	/**
	 * Date value.
	 * <ul>
	 * </ul>
	 *
	 */
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name="UPDATE_TIME"   )
	protected Date updateTime = DateUtil.getUTCDate();

	/**
	 * Added by
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name="ADDED_BY_ID"   )
	protected Long addedByUserId;


	/**
	 * Last updated by
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name="UPD_BY_ID"   )
	protected Long updatedByUserId;


	/**
	 * Default constructor. This will set all the attributes to default value.
	 */
	public XXDBBase ( ) {
	}

	public int getMyClassType( ) {
	    return AppConstants.CLASS_TYPE_NONE;
	}

	public String getMyDisplayValue() {
		return null;
	}

	/**
	 * This method sets the value to the member attribute <b>id</b>.
	 * You cannot set null to the attribute.
	 * @param id Value to set member attribute <b>id</b>
	 */
	public void setId( Long id ) {
		this.id = id;
	}

	/**
	 * Returns the value for the member attribute <b>id</b>
	 * @return Long - value of member attribute <b>id</b>.
	 */
	public Long getId( ) {
		return this.id;
	}

	/**
	 * This method sets the value to the member attribute <b>createTime</b>.
	 * You cannot set null to the attribute.
	 * @param createTime Value to set member attribute <b>createTime</b>
	 */
	public void setCreateTime( Date createTime ) {
		this.createTime = createTime;
	}

	/**
	 * Returns the value for the member attribute <b>createTime</b>
	 * @return Date - value of member attribute <b>createTime</b>.
	 */
	public Date getCreateTime( ) {
		return this.createTime;
	}

	/**
	 * This method sets the value to the member attribute <b>updateTime</b>.
	 * You cannot set null to the attribute.
	 * @param updateTime Value to set member attribute <b>updateTime</b>
	 */
	public void setUpdateTime( Date updateTime ) {
		this.updateTime = updateTime;
	}

	/**
	 * Returns the value for the member attribute <b>updateTime</b>
	 * @return Date - value of member attribute <b>updateTime</b>.
	 */
	public Date getUpdateTime( ) {
		return this.updateTime;
	}

	/**
	 * This method sets the value to the member attribute <b>addedByUserId</b>.
	 * You cannot set null to the attribute.
	 * @param addedByUserId Value to set member attribute <b>addedByUserId</b>
	 */
	public void setAddedByUserId( Long addedByUserId ) {
		this.addedByUserId = addedByUserId;
	}

	/**
	 * Returns the value for the member attribute <b>addedByUserId</b>
	 * @return Long - value of member attribute <b>addedByUserId</b>.
	 */
	public Long getAddedByUserId( ) {
		return this.addedByUserId;
	}


	/**
	 * This method sets the value to the member attribute <b>updatedByUserId</b>.
	 * You cannot set null to the attribute.
	 * @param updatedByUserId Value to set member attribute <b>updatedByUserId</b>
	 */
	public void setUpdatedByUserId( Long updatedByUserId ) {
		this.updatedByUserId = updatedByUserId;
	}

	/**
	 * Returns the value for the member attribute <b>updatedByUserId</b>
	 * @return Long - value of member attribute <b>updatedByUserId</b>.
	 */
	public Long getUpdatedByUserId( ) {
		return this.updatedByUserId;
	}


	/**
	 * This return the bean content in string format
	 * @return formatedStr
	*/
	@Override
	public String toString( ) {
		String str = "XXDBBase={";
		str += "id={" + id + "} ";
		str += "createTime={" + createTime + "} ";
		str += "updateTime={" + updateTime + "} ";
		str += "addedByUserId={" + addedByUserId + "} ";
		str += "updatedByUserId={" + updatedByUserId + "} ";
		str += "}";
		return str;
	}

	/**
	 * Checks for all attributes except referenced db objects
	 * @return true if all attributes match
	*/
	@Override
	public boolean equals( Object obj) {
		XXDBBase other = (XXDBBase) obj;
        	if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
            		return false;
        	}
        	if ((this.createTime == null && other.createTime != null) || (this.createTime != null && !this.createTime.equals(other.createTime))) {
            		return false;
        	}
        	if ((this.updateTime == null && other.updateTime != null) || (this.updateTime != null && !this.updateTime.equals(other.updateTime))) {
            		return false;
        	}
        	if ((this.addedByUserId == null && other.addedByUserId != null) || (this.addedByUserId != null && !this.addedByUserId.equals(other.addedByUserId))) {
            		return false;
        	}
        	if ((this.updatedByUserId == null && other.updatedByUserId != null) || (this.updatedByUserId != null && !this.updatedByUserId.equals(other.updatedByUserId))) {
            		return false;
        	}
		return true;
	}
	public static String getEnumName(String fieldName ) {
		//Later TODO
		//return super.getEnumName(fieldName);
		return null;
	}

}
