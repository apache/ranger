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

@JsonAutoDetect(getterVisibility= JsonAutoDetect.Visibility.NONE, setterVisibility= JsonAutoDetect.Visibility.NONE, fieldVisibility= JsonAutoDetect.Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)

public  class RangerTag extends RangerBaseModelObject {
	private static final long serialVersionUID = 1L;

	private String              name;
	private Map<String, String> attributeValues;

	public RangerTag(String guid, String name, Map<String, String> attributeValues) {
		super();

		setGuid(guid);
		setName(name);
		setAttributeValues(attributeValues);
	}

	public RangerTag(String name, Map<String, String> attributeValues) {
		this(null, name, attributeValues);
	}

	public RangerTag() {
		this(null, null, null);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Map<String, String> getAttributeValues() {
		return attributeValues;
	}

	public void setAttributeValues(Map<String, String> attributeValues) {
		this.attributeValues = attributeValues == null ? new HashMap<String, String>() : attributeValues;
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

		sb.append("name={").append(name).append("} ");

		sb.append("attributeValues={");
		if (attributeValues != null) {
			for (Map.Entry<String, String> e : attributeValues.entrySet()) {
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

