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
 * Role of the user
 * 
 */

import java.util.*;
import javax.persistence.*;
import javax.xml.bind.annotation.*;
import com.xasecure.common.*;
import com.xasecure.entity.*;


@Entity
@Table(name="x_portal_user_role")
@XmlRootElement
public class XXPortalUserRole extends XXDBBase implements java.io.Serializable {
	private static final long serialVersionUID = 1L;



	/**
	 * Id of the user
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name="USER_ID"  , nullable=false )
	protected Long userId;


	/**
	 * Role of the user
	 * <ul>
	 * <li>The maximum length for this attribute is <b>128</b>.
	 * </ul>
	 *
	 */
	@Column(name="USER_ROLE"   , length=128)
	protected String userRole;

	/**
	 * Status
	 * <ul>
	 * <li>This attribute is of type enum CommonEnums::ActiveStatus
	 * </ul>
	 *
	 */
	@Column(name="STATUS"  , nullable=false )
	protected int status = XAConstants.STATUS_DISABLED;

	/**
	 * Default constructor. This will set all the attributes to default value.
	 */
	public XXPortalUserRole ( ) {
		status = XAConstants.STATUS_DISABLED;
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
	 * This method sets the value to the member attribute <b>userRole</b>.
	 * You cannot set null to the attribute.
	 * @param userRole Value to set member attribute <b>userRole</b>
	 */
	public void setUserRole( String userRole ) {
		this.userRole = userRole;
	}

	/**
	 * Returns the value for the member attribute <b>userRole</b>
	 * @return String - value of member attribute <b>userRole</b>.
	 */
	public String getUserRole( ) {
		return this.userRole;
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
	 * This return the bean content in string format
	 * @return formatedStr
	*/
	@Override
	public String toString( ) {
		String str = "XXPortalUserRole={";
		str += super.toString();
		str += "userId={" + userId + "} ";
		str += "userRole={" + userRole + "} ";
		str += "status={" + status + "} ";
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
		XXPortalUserRole other = (XXPortalUserRole) obj;
        	if ((this.userId == null && other.userId != null) || (this.userId != null && !this.userId.equals(other.userId))) {
            		return false;
        	}
        	if ((this.userRole == null && other.userRole != null) || (this.userRole != null && !this.userRole.equals(other.userRole))) {
            		return false;
        	}
		if( this.status != other.status ) return false;
		return true;
	}
	public static String getEnumName(String fieldName ) {
		if( fieldName.equals("status") ) {
			return "CommonEnums.ActiveStatus";
		}
		//Later TODO
		//return super.getEnumName(fieldName);
		return null;
	}

}
