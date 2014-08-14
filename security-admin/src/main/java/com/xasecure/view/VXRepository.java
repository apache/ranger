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
 * Repository
 * @author tushar
 */

import com.xasecure.common.*;

import javax.xml.bind.annotation.*;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
public class VXRepository extends VXDataObject implements java.io.Serializable {
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
	 * Type of asset. i.e HDFS, HIVE, HBASE, KNOX
	 */
	protected String repositoryType;
	/**
	 * Config in json format
	 */
	protected String config;
	/**
	 * Status This attribute is of type boolean : true/false
	 */
	protected boolean isActive;
	/**
	 * Version No of Project
	 */
	protected String version;

	/**
	 * Default constructor. This will set all the attributes to default value.
	 */
	public VXRepository() {
		isActive = false;
		repositoryType = "";

	}

	/**
	 * This method sets the value to the member attribute <b>name</b>. You
	 * cannot set null to the attribute.
	 * 
	 * @param name
	 *            Value to set member attribute <b>name</b>
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Returns the value for the member attribute <b>name</b>
	 * 
	 * @return String - value of member attribute <b>name</b>.
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * This method sets the value to the member attribute <b>description</b>.
	 * You cannot set null to the attribute.
	 * 
	 * @param description
	 *            Value to set member attribute <b>description</b>
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * Returns the value for the member attribute <b>description</b>
	 * 
	 * @return String - value of member attribute <b>description</b>.
	 */
	public String getDescription() {
		return this.description;
	}

	/**
	 * This method sets the value to the member attribute <b>repositoryType</b>.
	 * You cannot set null to the attribute.
	 * 
	 * @param repositoryType
	 *            Value to set member attribute <b>repositoryType</b>
	 */
	public void setRepositoryType(String repositoryType) {
		this.repositoryType = repositoryType;
	}

	/**
	 * Returns the value for the member attribute <b>repositoryType</b>
	 * 
	 * @return String - value of member attribute <b>repositoryType</b>.
	 */
	public String getRepositoryType() {
		return this.repositoryType;
	}

	/**
	 * This method sets the value to the member attribute <b>config</b>. You
	 * cannot set null to the attribute.
	 * 
	 * @param config
	 *            Value to set member attribute <b>config</b>
	 */
	public void setConfig(String config) {
		this.config = config;
	}

	/**
	 * Returns the value for the member attribute <b>config</b>
	 * 
	 * @return String - value of member attribute <b>config</b>.
	 */
	public String getConfig() {
		return this.config;
	}

	/**
	 * This method sets the value to the member attribute <b>isActive</b>. You
	 * cannot set null to the attribute.
	 * 
	 * @param isActive
	 *            Value to set member attribute <b>isActive</b>
	 */
	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}

	/**
	 * Returns the value for the member attribute <b>isActive</b>
	 * 
	 * @return boolean - value of member attribute <b>isActive</b>.
	 */
	public boolean isActive() {
		return this.isActive;
	}

	/**
	 * Returns the value for the member attribute <b>version</b>
	 * 
	 * @return String - value of member attribute <b>version</b>.
	 */
	public String getVersion() {
		return version;
	}

	/**
	 * This method sets the value to the member attribute <b>version</b>. You
	 * cannot set null to the attribute.
	 * 
	 * @param version
	 *            Value to set member attribute <b>version</b>
	 */
	public void setVersion(String version) {
		this.version = version;
	}

	@Override
	public int getMyClassType() {
		return AppConstants.CLASS_TYPE_XA_ASSET;
	}

	/**
	 * This return the bean content in string format
	 * 
	 * @return formatedStr
	 */
	public String toString() {
		String str = "VXAsset={";
		str += super.toString();
		str += "name={" + name + "} ";
		str += "description={" + description + "} ";
		str += "isActive={" + isActive + "} ";
		str += "repositoryType={" + repositoryType + "} ";
		str += "config={" + config + "} ";
		str += "version={" + version + "} ";
		str += "}";
		return str;
	}
}
