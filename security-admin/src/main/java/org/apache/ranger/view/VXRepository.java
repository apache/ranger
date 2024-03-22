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

 package org.apache.ranger.view;


/**
 * Repository
 */

import org.apache.ranger.common.AppConstants;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
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
	public void setIsActive(boolean isActive) {
		this.isActive = isActive;
	}

	/**
	 * Returns the value for the member attribute <b>isActive</b>
	 *
	 * @return boolean - value of member attribute <b>isActive</b>.
	 */
	public boolean getIsActive() {
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
