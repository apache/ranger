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

 define([
	'backbone',
	'backbone.marionette'
],
function(Backbone){
    'use strict';

	return Backbone.Marionette.AppRouter.extend({
		/** Backbone routes hash */
		appRoutes: {
			""							: "serviceManagerAction",//"dashboardAction",
			"!/policymanager"			: "serviceManagerAction",

			/* HDFS related */
			"!/hdfs"					: "hdfsManageAction",
			"!/hdfs/:id/policies"		: "hdfsManageAction",
			"!/policy/:assetId/create"	: "policyCreateAction",
			"!/policy/:id/edit"			: "policyEditAction",
			"!/hdfs/:assetId/policy/:id": "policyViewAction",
			
			/****** Hive related **********************/
			"!/hive"						: "hiveManageAction",
			"!/hive/:id/policies"			: "hiveManageAction",
			"!/hive/:assetId/policy/create"	: "hivePolicyCreateAction",
			"!/hive/:assetId/policy/:id"	: "hivePolicyEditAction",
			
			/****** HBASE related **********************/
			"!/hbase"						: "hbaseManageAction",
			"!/hbase/:id/policies"			: "hbaseManageAction",
			"!/hbase/:assetId/policy/create": "hbasePolicyCreateAction",
			"!/hbase/:assetId/policy/:id"	: "hbasePolicyEditAction",
			
			/****** KNOX related ************************/
			"!/knox/:id/policies"			: "knoxManageAction",
			"!/knox/:assetId/policy/create"	: "knoxPolicyCreateAction",
			"!/knox/:assetId/policy/:id"	: "knoxPolicyEditAction",
			
			/****** STORM related ************************/
			"!/storm/:id/policies"			: "stormManageAction",
			"!/storm/:assetId/policy/create": "stormPolicyCreateAction",
			"!/storm/:assetId/policy/:id"	: "stormPolicyEditAction",
			
			/****** Asset related **********************/
			"!/asset"					: "policyManagerAction",
			"!/asset/create/:assetType" : "assetCreateAction",
			"!/asset/:id"				: "assetEditAction",
			
			/****** Analytics Report related **********************/
			"!/reports/userAccess"		: "userAccessReportAction",
			
			/****** Audit Report related **********************/
			"!/reports/audit/:tab"					: "auditReportAction",
			"!/reports/audit/loginSession/:paramName/:id"	: "loginSessionDetail",
			
			/****** User Profile related **********************/
			"!/userprofile"		: "userProfileAction",
			
			"!/users/:tab"		: "userManagerAction",
			"!/user/create"		: "userCreateAction",
			"!/user/:id"		: "userEditAction",
			
			"!/group/create"	: "groupCreateAction",
			"!/group/:id"		: "groupEditAction",

			/************GENERIC UI *****************************************/
			/****** Service related **********************/
			"!/service/:serviceType/create" 	: "serviceCreateAction",
			"!/service/:serviceType/edit/:id"	: "serviceEditAction",
			
			"!/service/:serviceId/policies"			: "policyManageAction",
			"!/service/:serviceId/policies/create"	: "RangerPolicyCreateAction",
			"!/service/:serviceId/policies/:id/edit": "RangerPolicyEditAction"
			
		}
	});
});
