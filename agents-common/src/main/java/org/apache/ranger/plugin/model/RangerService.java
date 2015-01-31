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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;


@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RangerService extends RangerBaseModelObject implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	private String              type             = null;
	private String              name             = null;
	private String              description      = null;
	private Map<String, String> configs          = null;
	private Long                policyVersion    = null;
	private Date                policyUpdateTime = null;


	/**
	 * @param type
	 */
	public RangerService() {
		this(null, null, null, null);
	}

	/**
	 * @param type
	 * @param name
	 * @param description
	 * @param configs
	 */
	public RangerService(String type, String name, String description, Map<String, String> configs) {
		super();

		setType(type);
		setName(name);
		setDescription(description);
		setConfigs(configs);
	}

	/**
	 * @param other
	 */
	public void updateFrom(RangerService other) {
		super.updateFrom(other);

		setType(other.getType());
		setName(other.getName());
		setDescription(other.getDescription());
		setConfigs(other.getConfigs());
	}

	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the configs
	 */
	public Map<String, String> getConfigs() {
		return configs;
	}

	/**
	 * @param configs the configs to set
	 */
	public void setConfigs(Map<String, String> configs) {
		if(this.configs == null) {
			this.configs = new HashMap<String, String>();
		}

		if(this.configs == configs) {
			return;
		}
		
		this.configs.clear();

		if(configs != null) {
			for(Map.Entry<String, String> e : configs.entrySet()) {
				this.configs.put(e.getKey(), e.getValue());
			}
		}
	}

	/**
	 * @return the policyVersion
	 */
	public Long getPolicyVersion() {
		return policyVersion;
	}

	/**
	 * @param policyVersion the policyVersion to set
	 */
	public void setPolicyVersion(Long policyVersion) {
		this.policyVersion = policyVersion;
	}

	/**
	 * @return the policyUpdateTime
	 */
	public Date getPolicyUpdateTime() {
		return policyUpdateTime;
	}

	/**
	 * @param policyUpdateTime the policyUpdateTime to set
	 */
	public void setPolicyUpdateTime(Date policyUpdateTime) {
		this.policyUpdateTime = policyUpdateTime;
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerService={");

		super.toString(sb);
		sb.append("name={").append(name).append("} ");
		sb.append("type={").append(type).append("} ");
		sb.append("description={").append(description).append("} ");

		sb.append("configs={");
		if(configs != null) {
			for(Map.Entry<String, String> e : configs.entrySet()) {
				sb.append(e.getKey()).append("={").append(e.getValue()).append("} ");
			}
		}
		sb.append("} ");

		sb.append("policyVersion={").append(policyVersion).append("} ");
		sb.append("policyUpdateTime={").append(policyUpdateTime).append("} ");

		sb.append("}");

		return sb;
	}
}
