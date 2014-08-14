define([
	'backbone',
	'backbone.marionette'
],
function(Backbone){
    'use strict';

	return Backbone.Marionette.AppRouter.extend({
		/** Backbone routes hash */
		appRoutes: {
			""							: "policyManagerAction",//"dashboardAction",
			"!/policymanager"			: "policyManagerAction",

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
			"!/group/:id"		: "groupEditAction"
		}
	});
});
