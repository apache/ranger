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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RangerResourceImpl implements RangerResource {
	private String              ownerUser = null;
	private Map<String, Object> elements  = null;


	public RangerResourceImpl() {
	}

	@Override
	public String getOwnerUser() {
		return ownerUser;
	}

	@Override
	public String getElementValue(String type) {
		String ret = null;

		if(elements != null) {
			Object value = elements.get(type);

			if(value != null) {
				if(value instanceof String) {
					ret = (String)value;
				} else { // value must be a List<String>
					@SuppressWarnings("unchecked")
					List<String> list = (List<String>)value;

					if(list != null && list.size() > 0) {
						ret = list.get(0);
					}
				}
			}
		}

		return ret;
	}

	@Override
	public List<String> getElementValues(String type) {
		List<String> ret = null;

		if(elements != null) {
			Object value = elements.get(type);
			
			if(value != null) {
				if(value instanceof String) {
					ret = new ArrayList<String>();
					ret.add((String)value);
				} else { // value must be a List<String>
					@SuppressWarnings("unchecked")
					List<String> tmpList = (List<String>)value;

					ret = tmpList;
				}
			}
		}

		return ret;
	}

	public void setOwnerUser(String ownerUser) {
		this.ownerUser = ownerUser;
	}

	public void setElement(String type, String value) {
		if(elements == null) {
			elements = new HashMap<String, Object>();
		}

		elements.put(type, value);
	}

	public void setElement(String type, List<String> value) {
		if(elements == null) {
			elements = new HashMap<String, Object>();
		}

		elements.put(type, value);
	}

	public void addElement(String type, String value) {
		if(elements == null) {
			elements = new HashMap<String, Object>();
		}

		Object val = elements.get(type);

		if(val == null) {
			elements.put(type, value);
		} else {
			List<String> list = null;

			if(val instanceof String) { // convert to a list-value
				list = new ArrayList<String>();

				elements.put(type,  list);

				list.add((String)val);
			} else { // value must be a List<String>
				@SuppressWarnings("unchecked")
				List<String> tmpList = (List<String>)val;
				
				list = tmpList;
			}
			
			list.add(value);
		}

	}
}
