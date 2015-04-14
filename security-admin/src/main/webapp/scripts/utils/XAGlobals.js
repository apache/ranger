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

 
define(function(require){
	'use strict';
	
	var XAGlobals = {};
	
	XAGlobals.settings = {};
	XAGlobals.settings.PAGE_SIZE = 25;
	XAGlobals.settings.AUDIT_REPORT_POLLING = 100000;
	XAGlobals.settings.uploadDefaultOpts = {
		disableImageResize: false,
		maxFileSize: 5000000,
		autoUpload : false
		//maxNumberOfFiles : 2
	};
	XAGlobals.settings.MAX_VALUE = 2147483647;

	XAGlobals.keys = {};
	XAGlobals.keys.ENTER_KEY = 13;
	XAGlobals.keys.ESC_KEY = 27;

	//XAGlobals.baseURL = '../';
	XAGlobals.baseURL = 'service/';

	XAGlobals.version = 0;//0 : production version 1: any other
	XAGlobals.AppTabs = {
			Dashboard 			: { value:1, valStr: 'Dashboard'},
			PolicyManager		: { value:2, valStr: 'Policy'},
			Users 				: { value:3, valStr: 'Users'},
//			Reports 			: { value:4, valStr: 'Reports'},
			Config 				: { value:5, valStr: 'Config'},
			Assets				: { value:6, valStr: 'Assets'},
			Analytics			: { value:7, valStr: 'Analytics'},
			Audit				: { value:8, valStr: 'Analytics'},
			Permissions			: { value:9, valStr:'Permissions'},
			None				: { value:10, valStr: 'None'}
		};

	XAGlobals.BooleanValue = {
		BOOL_TRUE:{value:"true", label:'True'},
		BOOL_FALSE:{value:"false", label:'False'}
	};
	XAGlobals.hardcoded = {};
	XAGlobals.hardcoded.HDFSAssetId = 1;
	XAGlobals.hardcoded.HBaseAssetId = 2;
	XAGlobals.hardcoded.HiveAssetId = 3;
	XAGlobals.DenyControllerActions = ['userManagerAction','userCreateAction','userEditAction','groupCreateAction',
	                                   'groupEditAction','auditReportAction','loginSessionDetail','serviceCreateAction','serviceEditAction','modulePermissionsAction','modulePermissionEditAction'];
	
	XAGlobals.ListOfModuleActions = {
									  'Policy Manager':['serviceManagerAction','serviceCreateAction','serviceEditAction', 'policyManageAction','RangerPolicyCreateAction','RangerPolicyEditAction'],
					  'Users/Groups' : ['userManagerAction','userCreateAction','userEditAction','groupCreateAction','groupEditAction'],
					  'Analytics' : ['userAccessReportAction'],
					  'Audit' : ['auditReportAction','loginSessionDetail'],
					  'Permissions' : ['modulePermissionsAction','modulePermissionEditAction']
									};
	return XAGlobals;
});
