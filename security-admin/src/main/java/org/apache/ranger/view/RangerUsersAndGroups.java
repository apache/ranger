/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.view;

import java.io.Serializable;
import java.util.List;

public class RangerUsersAndGroups implements Serializable {

    private List<String> users;
    private List<String> groups;
    private Boolean isAdmin = false;

    public RangerUsersAndGroups(){}

    public RangerUsersAndGroups(List<String> users, List<String> groups){
        this.users = users;
        this.groups = groups;
    }

    public RangerUsersAndGroups(List<String> users, List<String> groups, Boolean isAdmin){
        this.users = users;
        this.groups = groups;
        this.isAdmin = isAdmin;
    }

    public List<String> getUsers() {
        return users;
    }

    public void setUsers(List<String> users) {
        this.users = users;
    }

    public List<String> getGroups() {
        return groups;
    }

    public void setGroups(List<String> groups) {
        this.groups = groups;
    }

    public Boolean getAdmin() {
        return isAdmin;
    }

    public void setAdmin(Boolean admin) {
        isAdmin = admin;
    }

}
