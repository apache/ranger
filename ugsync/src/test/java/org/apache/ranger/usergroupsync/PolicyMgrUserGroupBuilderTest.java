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
import java.util.Set;

import org.apache.ranger.unixusersync.process.PolicyMgrUserGroupBuilder;

public class PolicyMgrUserGroupBuilderTest extends PolicyMgrUserGroupBuilder {
        private static int totalUsers = 0;
        //private static int totalGroups = 0;
        private Set<String> allGroups;

        @Override
        public void init() throws Throwable {
                // TODO Auto-generated method stub
                totalUsers = 0;
                allGroups = new HashSet<>();
        }

        @Override
        public void addOrUpdateUser(String user, List<String> groups) {
                totalUsers++;
                allGroups.addAll(groups);
                //System.out.println("Username: " + user + " and associated groups: " + groups);
        }
        
        @Override
        public void addOrUpdateGroup(String group) {
                //totalGroups++;
                allGroups.add(group);
        }

        public int getTotalUsers() {
                return totalUsers;
        }

        public int getTotalGroups() {
                //System.out.println("Groups = " + allGroups);
                return allGroups.size();
        }

}