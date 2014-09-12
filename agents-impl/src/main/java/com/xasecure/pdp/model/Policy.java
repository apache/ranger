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

 package com.xasecure.pdp.model;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.annotations.SerializedName;
import com.xasecure.pdp.config.gson.ExcludeSerialization;

public class Policy {
	
	public static final String RESOURCE_SPLITER = "," ;
	public static final String POLICY_ENABLED_STATUS = "Enabled" ;
	public static final String SELECTION_TYPE_INCLUSIVE = "Inclusion" ;
	public static final String SELECTION_TYPE_EXCLUSIVE = "Exclusion" ;
	
	//
	// Only for HDFS policies
	//
	private String resource ;
	@SerializedName("isRecursive")
	private int recursiveInd;
	
	// Only for Knox Policies
	//
		
	@SerializedName("topology_name")
	private String topologies ;
		
	@SerializedName("service_name")
	private String services ;
		
	
	//
	// Only for Hive Policies
	//
	
	@SerializedName("database_name")
	private String databases ;
	
	@SerializedName("table_name")
	private String tables ;
	
	@SerializedName("udf_name")
	private String udfs ;
	
	@SerializedName("column_name")
	private String columns ;
	
	@SerializedName("column_families")
	private String columnfamilies ;
	
	//
	// Neede for all Policies
	//
	@SerializedName("permission")
	private List<RolePermission> permissions ;
	
	@SerializedName("audit")
	private int auditInd ;
	
	@SerializedName("encrypt")
	private int encryptInd ;
	
	@SerializedName("policyStatus")
	private String policyStatus; 
	
	@SerializedName("tablePolicyType")
	private String tableSelectionType ;

	@SerializedName("columnPolicyType")
	private String columnSelectionType ;

	// Derived fields for PolicyAnalysis
	@ExcludeSerialization
	private List<ResourcePath> resourceList ;
	@ExcludeSerialization
	private List<String> databaseList ;
	@ExcludeSerialization
	private List<String> tableList ;
	@ExcludeSerialization
	private List<String> udfList ;
	@ExcludeSerialization
	private List<String> columnList ;
	@ExcludeSerialization
	private List<String> columnFamilyList ;
	@ExcludeSerialization
	private List<String> topologyList ;
	@ExcludeSerialization
	private List<String> serviceList ;

	public Policy() {
		permissions = new ArrayList<RolePermission>() ;
	}
	
	
	public String getResource() {
		return resource;
	}
	
	public void setResource(String resource) {
		this.resource = resource;
	}
	
	public String getDatabases() {
		return databases;
	}
	
	public void setDatabases(String databases) {
		this.databases = databases;
	}
	
	public String getTables() {
		return tables;
	}
	
	public void setTables(String tables) {
		this.tables = tables;
	}
	
	public String gettopologies() {
		return topologies;
	}
	
	public void setTopologies(String topologies) {
		this.topologies = topologies;
	}
	
	public String getServices() {
		return services;
	}
	
	public void setServices(String services) {
		this.services = services;
	}
	public String getUdfs() {
		return udfs;
	}

	public void setUdfs(String udfs) {
		this.udfs = udfs;
	}


	public String getColumns() {
		return columns;
	}
	public void setColumns(String columns) {
		this.columns = columns;
	}
	public String getColumnfamilies() {
		return columnfamilies;
	}
	public void setColumnfamilies(String columnfamilies) {
		this.columnfamilies = columnfamilies;
	}
	
	public List<RolePermission> getPermissions() {
		return permissions;
	}
	public void setPermissions(List<RolePermission> permissions) {
		this.permissions = permissions;
	}
	
	public int getRecursiveInd() {
		return recursiveInd;
	}
	public void setRecursiveInd(int recursiveInd) {
		this.recursiveInd = recursiveInd;
	}
	
	public int getAuditInd() {
		return auditInd;
	}


	public void setAuditInd(int auditInd) {
		this.auditInd = auditInd;
	}


	public int getEncryptInd() {
		return encryptInd;
	}


	public void setEncryptInd(int encryptInd) {
		this.encryptInd = encryptInd;
	}
	
	public String getPolicyStatus() {
		return policyStatus;
	}


	public void setPolicyStatus(String policyStatus) {
		this.policyStatus = policyStatus;
	}
	
	public String getTableSelectionType() {
		return tableSelectionType;
	}


	public void setTableSelectionType(String tableSelectionType) {
		this.tableSelectionType = tableSelectionType;
	}


	public String getColumnSelectionType() {
		return columnSelectionType;
	}


	public void setColumnSelectionType(String columnSelectionType) {
		this.columnSelectionType = columnSelectionType;
	}
	
	public boolean isTableSelectionExcluded() {
		return (this.tableSelectionType != null && SELECTION_TYPE_EXCLUSIVE.equalsIgnoreCase(this.tableSelectionType)) ;
	}

	public boolean isColumnSelectionExcluded() {
		return (this.columnSelectionType != null && SELECTION_TYPE_EXCLUSIVE.equalsIgnoreCase(this.columnSelectionType)) ;
	}


	// An older version of policy manager would show policyStatus as NULL (considered that as Enabled)
	public boolean isEnabled() {
		return (this.policyStatus == null  ||  POLICY_ENABLED_STATUS.equalsIgnoreCase(this.policyStatus)) ;
	}

	public List<ResourcePath> getResourceList() {
		if (this.resourceList == null) {
			this.resourceList = getResourceList(resource) ;
		}
		return this.resourceList;
	}
	public List<String> getDatabaseList() {
		if (this.databaseList == null) {
			this.databaseList = getList(this.databases) ;
		}
		return this.databaseList;
	}
	public List<String> getTableList() {
		if (this.tableList == null) {
			this.tableList = getList(this.tables) ;
		}
		return this.tableList;
	}
	public List<String> getColumnList() {
		if (this.columnList == null) {
			this.columnList = getList(this.columns) ;
		}
		return this.columnList;
	}
	public List<String> getColumnFamilyList() {
		if (this.columnFamilyList  == null) {
			this.columnFamilyList = getList(this.columnfamilies) ;
		}
		return this.columnFamilyList;
	}
	public List<String> getUDFList() {
		if (this.udfList  == null && this.udfList != null) {
			this.udfList = getList(this.udfs) ;
		}
		return this.udfList;
	}

	public List<String> getTopologyList() {
		if (this.topologyList  == null) {
			this.topologyList = getList(this.topologies) ;
		}
		return this.topologyList;
	}
	
	public List<String> getServiceList() {
		if (this.serviceList  == null) {
			this.serviceList = getList(this.services) ;
		}
		return this.serviceList;
	}
	
	
	private List<String> getList(String resource) {
		List<String> ret = new ArrayList<String>() ;
		if (resource == null || resource.trim().isEmpty()) {
			resource = "*" ;
		}
		for(String r :  resource.split(RESOURCE_SPLITER)) {
			ret.add(r) ;
		}
		
		return ret;
	}
	
	private List<ResourcePath> getResourceList(String resource) {
		List<ResourcePath> ret = new ArrayList<ResourcePath>() ;
		if (resource != null && ! resource.isEmpty()) {
			for(String path :  resource.split(RESOURCE_SPLITER)) {
				ret.add(new ResourcePath(path)) ;
			}
		}
		return ret ;
	}
	
}
