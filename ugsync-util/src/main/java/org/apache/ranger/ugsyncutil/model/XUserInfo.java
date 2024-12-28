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

package org.apache.ranger.ugsyncutil.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XUserInfo {
    private String              id;
    private String              name;
    private String              firstName;
    private String              description;
    private String              otherAttributes;
    private String              syncSource;
    private String              userSource;
    private String              status;
    private String              isVisible;
    private List<String>        groupNameList = new ArrayList<>();
    private List<String>        userRoleList  = new ArrayList<>();
    private Map<String, String> otherAttrsMap = new HashMap<>();

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

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getUserSource() {
        return userSource;
    }

    public void setUserSource(String userSource) {
        this.userSource = userSource;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getIsVisible() {
        return isVisible;
    }

    public void setIsVisible(String isVisible) {
        this.isVisible = isVisible;
    }

    public List<String> getGroupNameList() {
        return groupNameList;
    }

    public void setGroupNameList(List<String> groupNameList) {
        this.groupNameList = groupNameList;
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

    public Map<String, String> getOtherAttrsMap() {
        return otherAttrsMap;
    }

    public void setOtherAttrsMap(Map<String, String> otherAttrsMap) {
        if (otherAttrsMap != null) {
            this.otherAttrsMap = otherAttrsMap;
        }
    }

    public String getOtherAttributes() {
        return otherAttributes;
    }

    public void setOtherAttributes(String otherAttributes) {
        this.otherAttributes = otherAttributes;
    }

    public String getSyncSource() {
        return syncSource;
    }

    public void setSyncSource(String syncSource) {
        this.syncSource = syncSource;
    }

    @Override
    public int hashCode() {
        final int prime  = 31;
        int       result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        XUserInfo userInfo = (XUserInfo) o;
        if (name == null) {
            return userInfo.name == null;
        } else {
            return name.equals(userInfo.name);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("XUserInfo [");
        sb.append("id=").append(id).append(", ");
        sb.append("name=").append(name).append(", ");
        sb.append("firstName=").append(firstName).append(", ");
        sb.append("description=").append(description).append(", ");
        sb.append("groupNameList=").append(groupNameList).append(", ");
        sb.append("userRoleList=").append(userRoleList);
        sb.append("]");
        return sb.toString();
    }
}
