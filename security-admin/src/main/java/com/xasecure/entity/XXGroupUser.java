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
 * Group of users
 * 
 */

import java.util.*;
import javax.persistence.*;
import javax.xml.bind.annotation.*;
import com.xasecure.common.*;
import com.xasecure.entity.*;


@Entity
@Table(name="x_group_users")
@XmlRootElement
public class XXGroupUser extends XXDBBase implements java.io.Serializable {
	private static final long serialVersionUID = 1L;



	/**
	 * Name
	 * <ul>
	 * <li>The maximum length for this attribute is <b>1024</b>.
	 * </ul>
	 *
	 */
	@Column(name="GROUP_NAME"  , nullable=false , length=1024)
	protected String name;

	/**
	 * Id of the group
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name="P_GROUP_ID"   )
	protected Long parentGroupId;


	/**
	 * Id of the user
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name="USER_ID"   )
	protected Long userId;


	/**
	 * Default constructor. This will set all the attributes to default value.
	 */
	public XXGroupUser ( ) {
	}

	@Override
	public int getMyClassType( ) {
	    return AppConstants.CLASS_TYPE_XA_GROUP_USER;
	}

	@Override
	public String getMyDisplayValue() {
		return getName( );
	}

	/**
	 * This method sets the value to the member attribute <b>name</b>.
	 * You cannot set null to the attribute.
	 * @param name Value to set member attribute <b>name</b>
	 */
	public void setName( String name ) {
		this.name = name;
	}

	/**
	 * Returns the value for the member attribute <b>name</b>
	 * @return String - value of member attribute <b>name</b>.
	 */
	public String getName( ) {
		return this.name;
	}

	/**
	 * This method sets the value to the member attribute <b>parentGroupId</b>.
	 * You cannot set null to the attribute.
	 * @param parentGroupId Value to set member attribute <b>parentGroupId</b>
	 */
	public void setParentGroupId( Long parentGroupId ) {
		this.parentGroupId = parentGroupId;
	}

	/**
	 * Returns the value for the member attribute <b>parentGroupId</b>
	 * @return Long - value of member attribute <b>parentGroupId</b>.
	 */
	public Long getParentGroupId( ) {
		return this.parentGroupId;
	}


	/**
	 * This method sets the value to the member attribute <b>userId</b>.
	 * You cannot set null to the attribute.
	 * @param userId Value to set member attribute <b>userId</b>
	 */
	public void setUserId( Long userId ) {
		this.userId = userId;
	}

	/**
	 * Returns the value for the member attribute <b>userId</b>
	 * @return Long - value of member attribute <b>userId</b>.
	 */
	public Long getUserId( ) {
		return this.userId;
	}


	/**
	 * This return the bean content in string format
	 * @return formatedStr
	*/
	@Override
	public String toString( ) {
		String str = "XXGroupUser={";
		str += super.toString();
		str += "name={" + name + "} ";
		str += "parentGroupId={" + parentGroupId + "} ";
		str += "userId={" + userId + "} ";
		str += "}";
		return str;
	}

	/**
	 * Checks for all attributes except referenced db objects
	 * @return true if all attributes match
	*/
	@Override
	public boolean equals( Object obj) {
		if ( !super.equals(obj) ) {
			return false;
		}
		XXGroupUser other = (XXGroupUser) obj;
        	if ((this.name == null && other.name != null) || (this.name != null && !this.name.equals(other.name))) {
            		return false;
        	}
        	if ((this.parentGroupId == null && other.parentGroupId != null) || (this.parentGroupId != null && !this.parentGroupId.equals(other.parentGroupId))) {
            		return false;
        	}
        	if ((this.userId == null && other.userId != null) || (this.userId != null && !this.userId.equals(other.userId))) {
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
