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

import java.util.HashMap;
import java.util.Map;


public class RangerResourceImpl implements RangerMutableResource {
	private String              ownerUser = null;
	private Map<String, String> elements  = null;


	public RangerResourceImpl() {
	}

	@Override
	public String getOwnerUser() {
		return ownerUser;
	}

	@Override
	public boolean elementExists(String type) {
		return elements != null && elements.containsKey(type);
	}

	@Override
	public String getElementValue(String type) {
		String ret = null;

		if(elements != null && elements.containsKey(type)) {
			ret = elements.get(type);
		}

		return ret;
	}

	@Override
	public void setOwnerUser(String ownerUser) {
		this.ownerUser = ownerUser;
	}

	@Override
	public void setElement(String type, String value) {
		// TODO: verify that leafElementType != type
		if(elements == null) {
			elements = new HashMap<String, String>();
		}

		elements.put(type, value);
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
