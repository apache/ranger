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

 package org.apache.ranger.pdp.model;

import java.util.ArrayList;
import java.util.List;

public class RolePermission {
	
	private List<String>	users ;
	private List<String> 	groups ;
	private List<String>	access ;
	private List<String>	ipAddress ;
	
	public RolePermission() {
		users  = new ArrayList<String>() ;
		groups = new ArrayList<String>() ;
		access = new ArrayList<String>() ;
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
	
	public List<String> getAccess() {
		return this.access;
	}
	
	public List<String> getIpAddress() {
		return this.ipAddress;
	}
	
	public void setIpAddress(List<String> ipAddress) {
		this.ipAddress = ipAddress ;
	}
	
	public void setAccess(List<String> access) {
		this.access = access ;
	}
	
}
