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

package org.apache.ranger.plugin.policyengine;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ObjectUtils;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;


public class RangerAccessResourceImpl implements RangerMutableResource {
	private String              ownerUser        = null;
	private Map<String, String> elements         = null;
	private String              stringifiedValue = null;
	private String              leafName         = null;


	public RangerAccessResourceImpl() {
		this(null, null);
	}

	public RangerAccessResourceImpl(Map<String, String> elements) {
		this(elements, null);
	}

	public RangerAccessResourceImpl(Map<String, String> elements, String ownerUser) {
		this.elements  = elements;
		this.ownerUser = ownerUser;
	}

	@Override
	public String getOwnerUser() {
		return ownerUser;
	}

	@Override
	public boolean exists(String name) {
		return elements != null && elements.containsKey(name);
	}

	@Override
	public String getValue(String name) {
		String ret = null;

		if(elements != null && elements.containsKey(name)) {
			ret = elements.get(name);
		}

		return ret;
	}

	@Override
	public Set<String> getKeys() {
		Set<String> ret = null;

		if(elements != null) {
			ret = elements.keySet();
		}

		return ret;
	}

	@Override
	public void setOwnerUser(String ownerUser) {
		this.ownerUser = ownerUser;
	}

	@Override
	public void setValue(String name, String value) {
		if(value == null) {
			if(elements != null) {
				elements.remove(name);

				if(elements.isEmpty()) {
					elements = null;
				}
			}
		} else {
			if(elements == null) {
				elements = new HashMap<String, String>();
			}
			elements.put(name, value);
		}

		// reset, so that these will be computed again with updated elements
		stringifiedValue = leafName = null;
	}

	@Override
	public String getLeafName(RangerServiceDef serviceDef) {
		String ret = leafName;

		if(ret == null) {
			if(serviceDef != null && serviceDef.getResources() != null) {
				List<RangerResourceDef> resourceDefs = serviceDef.getResources();

				for(int idx = resourceDefs.size() - 1; idx >= 0; idx--) {
					RangerResourceDef resourceDef = resourceDefs.get(idx);

					if(resourceDef == null || !exists(resourceDef.getName())) {
						continue;
					}

					ret = leafName = resourceDef.getName();

					break;
				}
			}
		}

		return ret;
	}

	@Override
	public String getAsString(RangerServiceDef serviceDef) {
		String ret = stringifiedValue;

		if(ret == null) {
			if(serviceDef != null && serviceDef.getResources() != null) {
				StringBuilder sb = new StringBuilder();

				for(RangerResourceDef resourceDef : serviceDef.getResources()) {
					if(resourceDef == null || !exists(resourceDef.getName())) {
						continue;
					}

					if(sb.length() > 0) {
						sb.append(RESOURCE_SEP);
					}

					sb.append(getValue(resourceDef.getName()));
				}

				if(sb.length() > 0) {
					ret = stringifiedValue = sb.toString();
				}
			}
		}

		return ret;
	}

	@Override
	public Map<String, String> getAsMap() {
		return Collections.unmodifiableMap(elements);
	}

	@Override
	public boolean equals(Object obj) {
		if(obj == null || !(obj instanceof RangerAccessResourceImpl)) {
			return false;
		}

		if(this == obj) {
			return true;
		}

		RangerAccessResourceImpl other = (RangerAccessResourceImpl) obj;

		return ObjectUtils.equals(ownerUser, other.ownerUser) &&
			   ObjectUtils.equals(elements, other.elements);
	}

	@Override
	public int hashCode() {
		int ret = 7;

		ret = 31 * ret + ObjectUtils.hashCode(ownerUser);
		ret = 31 * ret + ObjectUtils.hashCode(elements);

		return ret;
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerResourceImpl={");

		sb.append("ownerUser={").append(ownerUser).append("} ");

		sb.append("elements={");
		if(elements != null) {
			for(Map.Entry<String, String> e : elements.entrySet()) {
				sb.append(e.getKey()).append("=").append(e.getValue()).append("; ");
			}
		}
		sb.append("} ");

		sb.append("}");

		return sb;
	}
}
