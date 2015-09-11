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

package org.apache.ranger.plugin.util;


import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ServiceTags implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	public static final String OP_ADD_OR_UPDATE = "add_or_update";
	public static final String OP_DELETE        = "delete";

	private String                      op;
	private String                      serviceName;
	private Long                        tagVersion;
	private Date                        tagUpdateTime;
	private Map<Long, RangerTagDef>     tagDefinitions;
	private Map<Long, RangerTag>        tags;
	private List<RangerServiceResource> serviceResources;
	private Map<Long, List<Long>>       resourceToTagIds;

	/**
	 * @return the op
	 */
	public String getOp() {
		return op;
	}

	/**
	 * @return the serviceName
	 */
	public String getServiceName() {
		return serviceName;
	}

	/**
	 * @param op the op to set
	 */
	public void setOp(String op) {
		this.op = op;
	}

	/**
	 * @param serviceName the serviceName to set
	 */
	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	/**
	 * @return the tagVersion
	 */
	public Long getTagVersion() {
		return tagVersion;
	}

	/**
	 * @param tagVersion the version to set
	 */
	public void setTagVersion(Long tagVersion) {
		this.tagVersion = tagVersion;
	}

	/**
	 * @return the tagUpdateTime
	 */
	public Date getTagUpdateTime() {
		return tagUpdateTime;
	}

	/**
	 * @param tagUpdateTime the tagUpdateTime to set
	 */
	public void setTagUpdateTime(Date tagUpdateTime) {
		this.tagUpdateTime = tagUpdateTime;
	}

	public Map<Long, RangerTagDef> getTagDefinitions() {
		return tagDefinitions;
	}

	public void setTagDefinitions(Map<Long, RangerTagDef> tagDefinitions) {
		this.tagDefinitions = tagDefinitions;
	}

	public Map<Long, RangerTag> getTags() {
		return tags;
	}

	public void setTags(Map<Long, RangerTag> tags) {
		this.tags = tags;
	}

	public List<RangerServiceResource> getServiceResources() {
		return serviceResources;
	}

	public void setServiceResources(List<RangerServiceResource> serviceResources) {
		this.serviceResources = serviceResources;
	}

	public Map<Long, List<Long>> getResourceToTagIds() {
		return resourceToTagIds;
	}

	public void setResourceToTagIds(Map<Long, List<Long>> resourceToTagIds) {
		this.resourceToTagIds = resourceToTagIds;
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("ServiceTags={").append("tagVersion=").append(tagVersion).append(", ")
				.append("tagUpdateTime={").append(tagUpdateTime).append("} ")
				.append("}");

		return sb;
	}

}
