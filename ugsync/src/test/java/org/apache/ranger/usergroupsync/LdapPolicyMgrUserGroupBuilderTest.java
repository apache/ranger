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

package org.apache.ranger.usergroupsync;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ranger.ldapusersync.process.LdapPolicyMgrUserGroupBuilder;

public class LdapPolicyMgrUserGroupBuilderTest extends LdapPolicyMgrUserGroupBuilder {
        private Set<String> allGroups;
        private Set<String> allUsers;

        @Override
        public void init() throws Throwable {
                allGroups = new HashSet<>();
                allUsers = new HashSet<>();
        }

        @Override
        public void addOrUpdateUser(String user, Map<String, String> userAttrs, List<String> groups) {
                allUsers.add(user);
                //System.out.println("Username: " + user + " and associated groups: " + groups);
        }

        @Override
        public void addOrUpdateGroup(String group, Map<String, String> groupAttrs) {
                allGroups.add(group);
                //System.out.println("Groupname: " + group);
        }
        
        @Override
        public void addOrUpdateUser(String user) {
                allUsers.add(user);
                //System.out.println("Username: " + user);
        }

        @Override
        public void addOrUpdateGroup(String group, Map<String, String> groupAttrs, List<String> users) {
        	boolean addGroup = false;
        		for (String user : users) {
        			if (allUsers.contains(user)) {
        				addGroup = true;
        				break;
        			}
        		}
        		if (addGroup) {
        			allGroups.add(group);
        		}
                //allUsers.addAll(users);
                //System.out.println("Groupname: " + group + " and associated users: " + users);
        }

        public int getTotalUsers() {
                return allUsers.size();
        }

        public int getTotalGroups() {
                //System.out.println("Groups = " + allGroups);
                return allGroups.size();
        }

        public Set<String> getAllGroups() {
                return allGroups;
        }

        public Set<String> getAllUsers() {
                return allUsers;
        }
}