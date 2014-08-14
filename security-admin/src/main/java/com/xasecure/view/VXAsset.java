package com.xasecure.view;
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
 * Asset
 *
 */

import java.util.*;

import com.xasecure.common.*;
import com.xasecure.common.view.*;

import com.xasecure.common.*;
import com.xasecure.json.JsonDateSerializer;

import com.xasecure.view.*;

import javax.xml.bind.annotation.*;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
public class VXAsset extends VXDataObject implements java.io.Serializable {
	private static final long serialVersionUID = 1L;


	/**
	 * Name
	 */
	protected String name;
	/**
	 * Description
	 */
	protected String description;
	/**
	 * Status
	 * This attribute is of type enum CommonEnums::ActiveStatus
	 */
	protected int activeStatus;
	/**
	 * Type of asset
	 * This attribute is of type enum CommonEnums::AssetType
	 */
	protected int assetType = AppConstants.ASSET_UNKNOWN;
	/**
	 * Config in json format
	 */
	protected String config;
	/**
	 * Support native authorization
	 */
	protected boolean supportNative = false;

	/**
	 * Default constructor. This will set all the attributes to default value.
	 */
	public VXAsset ( ) {
		activeStatus = 0;
		assetType = AppConstants.ASSET_UNKNOWN;
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
	 * This method sets the value to the member attribute <b>description</b>.
	 * You cannot set null to the attribute.
	 * @param description Value to set member attribute <b>description</b>
	 */
	public void setDescription( String description ) {
		this.description = description;
	}

	/**
	 * Returns the value for the member attribute <b>description</b>
	 * @return String - value of member attribute <b>description</b>.
	 */
	public String getDescription( ) {
		return this.description;
	}

	/**
	 * This method sets the value to the member attribute <b>activeStatus</b>.
	 * You cannot set null to the attribute.
	 * @param activeStatus Value to set member attribute <b>activeStatus</b>
	 */
	public void setActiveStatus( int activeStatus ) {
		this.activeStatus = activeStatus;
	}

	/**
	 * Returns the value for the member attribute <b>activeStatus</b>
	 * @return int - value of member attribute <b>activeStatus</b>.
	 */
	public int getActiveStatus( ) {
		return this.activeStatus;
	}

	/**
	 * This method sets the value to the member attribute <b>assetType</b>.
	 * You cannot set null to the attribute.
	 * @param assetType Value to set member attribute <b>assetType</b>
	 */
	public void setAssetType( int assetType ) {
		this.assetType = assetType;
	}

	/**
	 * Returns the value for the member attribute <b>assetType</b>
	 * @return int - value of member attribute <b>assetType</b>.
	 */
	public int getAssetType( ) {
		return this.assetType;
	}

	/**
	 * This method sets the value to the member attribute <b>config</b>.
	 * You cannot set null to the attribute.
	 * @param config Value to set member attribute <b>config</b>
	 */
	public void setConfig( String config ) {
		this.config = config;
	}

	/**
	 * Returns the value for the member attribute <b>config</b>
	 * @return String - value of member attribute <b>config</b>.
	 */
	public String getConfig( ) {
		return this.config;
	}

	/**
	 * This method sets the value to the member attribute <b>supportNative</b>.
	 * You cannot set null to the attribute.
	 * @param supportNative Value to set member attribute <b>supportNative</b>
	 */
	public void setSupportNative( boolean supportNative ) {
		this.supportNative = supportNative;
	}

	/**
	 * Returns the value for the member attribute <b>supportNative</b>
	 * @return boolean - value of member attribute <b>supportNative</b>.
	 */
	public boolean isSupportNative( ) {
		return this.supportNative;
	}

	@Override
	public int getMyClassType( ) {
	    return AppConstants.CLASS_TYPE_XA_ASSET;
	}

	/**
	 * This return the bean content in string format
	 * @return formatedStr
	*/
	public String toString( ) {
		String str = "VXAsset={";
		str += super.toString();
		str += "name={" + name + "} ";
		str += "description={" + description + "} ";
		str += "activeStatus={" + activeStatus + "} ";
		str += "assetType={" + assetType + "} ";
		str += "config={" + config + "} ";
		str += "supportNative={" + supportNative + "} ";
		str += "}";
		return str;
	}
}
