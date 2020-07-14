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

 package org.apache.ranger.unixusersync.model;

import java.util.ArrayList;
import java.util.List;

public class XUserInfo {
	private String id;
	private String name;
	private String 	description;
	private String otherAttributes;
    private List<String> groupNameList = new ArrayList<String>();
    private List<String> userRoleList = new ArrayList<String>();
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	
	public void setGroupNameList(List<String> groupNameList) {
		this.groupNameList = groupNameList;
	}
	
	public List<String> getGroupNameList() {
		return groupNameList;
	}

	public void deleteGroups(List<String> delGroups) {
		for (String delGroup : delGroups) {
			groupNameList.remove(delGroup);
		}
	}

	public List<String> getGroups() {
		return groupNameList;
	}

    public List<String> getUserRoleList() {
        return userRoleList;
    }

    public void setUserRoleList(List<String> userRoleList) {
        this.userRoleList = userRoleList;
    }

	public String getOtherAttributes() {
		return otherAttributes;
	}

	public void setOtherAttributes(String otherAttributes) {
		this.otherAttributes = otherAttributes;
	}

	@Override
    public String toString() {
        return "XUserInfo [id=" + id + ", name=" + name + ", description="
                + description + ", groupNameList=" + groupNameList
                + ", userRoleList=" + userRoleList + "]";
    }

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		XUserInfo userInfo = (XUserInfo) o;
		if (name == null) {
			if (userInfo.name != null)
				return false;
		} else if (!name.equals(userInfo.name))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}
}
