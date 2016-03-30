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

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;
import java.util.Map;

@JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RangerTag extends RangerBaseModelObject {
	private static final long serialVersionUID = 1L;

	public static final short OWNER_SERVICERESOURCE = 0;
	public static final short OWNER_GLOBAL          = 1;

	private String              type;
    private Short               owner = OWNER_SERVICERESOURCE;

	private Map<String, String> attributes;

	public RangerTag(String guid, String type, Map<String, String> attributes, Short owner) {
		super();

		setGuid(guid);
		setType(type);
		setOwner(owner);
		setAttributes(attributes);
		setOwner(owner);
	}

	public RangerTag(String type, Map<String, String> attributes) {
		this(null, type, attributes, OWNER_SERVICERESOURCE);
	}

	public RangerTag() {
		this(null, null, null, OWNER_SERVICERESOURCE);
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Map<String, String> getAttributes() {
		return attributes;
	}

	public void setAttributes(Map<String, String> attributes) {
		this.attributes = attributes == null ? new HashMap<String, String>() : attributes;
	}

	public Short getOwner() {
		return this.owner;
	}

	public void setOwner(Short owner) {
		this.owner = owner;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerTag={");

		super.toString(sb);

		sb.append("type={").append(type).append("} ");
		sb.append("owner={").append(owner).append("} ");

		sb.append("attributes={");
		if (attributes != null) {
			for (Map.Entry<String, String> e : attributes.entrySet()) {
				sb.append(e.getKey()).append("={");
				sb.append(e.getValue());
				sb.append("} ");
			}
		}
		sb.append("} ");

		sb.append(" }");

		return sb;
	}
}

