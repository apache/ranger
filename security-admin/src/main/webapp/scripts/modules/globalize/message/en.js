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

 /**
 * Never Delete any key without seraching it in all View and Template files
 */
/*(function( window, undefined ) {

var Globalize;

if ( typeof require !== "undefined" &&
	typeof exports !== "undefined" &&
	typeof module !== "undefined" ) {
	// Assume CommonJS
	Globalize = require( "../globalize.js" );
} else {
	// Global variable
	Globalize = window.Globalize;
}
*/
define(function(require) {
	'use strict';
	Globalize = require( "globalize" );

	Globalize.addCultureInfo( "en", {
        messages:                  {
        	// Form labels, Table headers etc
			lbl : {
				// Common
				// Accounts
				// MSLinks
				/*
				 * Menu related
				 */
				home 						: 'Home',
				name 						: 'Name',
				password					: 'Password',
				passwordConfirm				: 'Password Confirm',
				listOfPolicies 				: 'List of Policies',
				addNewPolicy 				: 'Add New Policy',
				resource					: 'Resource',
				action						: 'Action',
				result						: 'Result',
				enforcer					: 'Enforcer',
				date						: 'Date',
				resourcePath 				: 'Resource Path',
				includesAllPathsRecursively : 'Recursive',
				groups 						: 'Groups',
				group 						: 'Groups',
				auditLogging 				: 'Audit Logging',
				encrypted 					: 'Encrypt',
				resourceType 				: 'Resource Type',
				description 				: 'Description',
				groupPermissions			: 'Group Permissions',
				userPermissions				: 'User Permissions',
				selectGroup					: 'Select Group',
				admin						: 'Admin',
				execute						: 'Execute',
				create 						: 'Create',
				write						: 'Write',
				deletes						: 'Delete',
				read						: 'Read',
				select						: 'Select',
				update						: 'Update',
				drop						: 'Drop',
				alter						: 'Alter',
				index						: 'Index',
				lock						: 'Lock',
				all							: 'All',
				databaseName				: 'Database Name',
				tableName					: 'Table Name',
				columnName					: 'Column Name',
				columnFamilies				: 'Column Families',
				selectDatabaseName			: 'Select Database Name',
				selectTableName				: 'Select Table Name',
				selectColumnName			: 'Select Column Name',
				enterColumnName				: 'Enter Column Name',
				selectColumnFamilies		: 'Select Column Familes',
				resourceInformation 		: 'Resource Information',
				database					: 'Database'	,
				table						: 'Table',
				column						: 'Column',
				policyInfo					: 'Policy Information',
				createdBy 					: 'Created By',
				createdOn					: 'Created On',
				updatedBy					: 'Updated By',
				updatedOn					: 'Updated On',
				groupName					: 'Group Name',
				permissions					: 'Permissions',
				permissionGranted			: 'Permission Granted',
				createAccount 				: 'Create Account',
				editAccount 				: 'Edit Account ',
				selectFolder 				: 'Select Folder',		
				exports						: 'Export Policies',
				userName					: 'User Name',
				authToLocal					: 'authToLocal',
				dataNode					:'dataNode',
				nameNode					:'nameNode',
				secNamenode					:'secNamenode',
				userAccessReport				: 'User Access Report',
				auditReport					: 'Audit Report',
				createAsset					: 'Create Repository',
				editAsset					: 'Edit Repository',
				assetType					: 'Repository Type',
				selectUser					: 'Select User',
				listOfHDFSPolicies 			: 'List of HDFS Policies',
				listOfHIVEPolicies 			: 'List of HIVE Policies',
				listOfHBASEPolicies 		: 'List of HBASE Policies',
				listOfKNOXPolicies 			: 'List of KNOX Policies',
				listOfSTORMPolicies 		: 'List of STORM Policies',
				users						: 'Users',
				repository					: 'Repository',
				repositoryDetails 			: 'Repository Details',
				createRepository			: 'Create Repository',
				
				firstName					: 'First Name',
				lastName					: 'Last Name',
				email 						: 'Email',
				emailAddress    			: 'Email Address',
				newPassword     			: 'New Password',
				reEnterPassword 			: 'Re-enter New Password',
				oldPassword					: 'Old Password',
				
				customerName 				: 'Customer Name',
				accountCode						: 'Account Code',
				accountStatus					: 'Account Status',
				ActiveStatus_STATUS_ENABLED 	: 'Enabled',
				ActiveStatus_STATUS_DISABLED 	: 'Disabled',
				visibility						: 'Visibility',
				VisibilityStatus_IS_VISIBLE     : 'Visible',
				VisibilityStatus_IS_HIDDEN      : 'Hidden',
				commonNameForCertificate 		: 'Common Name For Certificate',
				status							: 'Status',
				userListing						: 'User List',
				userInfo						: 'User Info',
				userEdit						: 'User Edit',
				userCreate						: 'User Create',
				groupEdit						: 'Group Edit',
				groupCreate						: 'Group Create',
				addNewUser						: 'Add New User',
				addNewGroup						: 'Add New Group',
				selectUserDefinedFunction		: 'Select UDF',
				selectView						: 'Select View',
				udfName							: 'UDF Name',
				viewName						: 'View Name',
				permForTable					: 'Permission For Tables',
				permForView					 	: 'Permission For Views',
				permForUdf						: 'Permission For User Defined Function',
				policyStatus					: 'Policy Status',
				httpResponseCode				: 'Http Response Code',
				repositoryName					: 'Repository Name',
				agentId							: 'Plugin Id',
				agentIp							: 'Plugin IP',
				createDate						: 'Export Date',
				attributeName 					: 'Attribute Name',
				policyType						: 'Policy Type',
				previousValue					: 'Previous Value',
				newValue						: 'New Value',
				udf								: 'UDF',
				tableType						: 'Table Type',
				columnType						: 'Column Type',
				accountName						: 'Account Name',
				createdDate						: 'Created Date',
				sessionId						: 'Session Id',
				operation						: 'Operation',
				auditType						: 'Audit Type',
				user							: 'User',
				actions							: 'Actions',
				loginId							: 'Login Id',
				loginType						: 'Login Type',
				ip								: 'IP',
				userAgent						: 'User Agent',
				loginTime						: 'Login Time',
				sessionDetail					: 'Session Detail',
				ok								: 'OK',
				id								: 'ID',
				type							: 'Type',
				resourceId						: 'Resource ID',
				eventTime						: 'Event Time',
				resourceName					: 'Resource Name',
				repoType						: 'Repository Type',
				accessType						: 'Access Type',
				aclEnforcer						: 'Access Enforcer',	
				active							: 'Active',
				selectRole						: 'Select Role',
				role							: 'Role',
				userSource						: 'User Source',
				groupSource						: 'Group Source',
				policyName						: 'Policy Name',
				allow							: 'Allow',
				allowAccess							: 'Allow Access',
				selectTopologyName				: 'Select Topology Name',
				selectServiceName				: 'Select Service Name',
				topologyName					: 'Topology Name',
				serivceName						: 'Service Name',
				serivceType						: 'Service Type',
				ipAddress						: 'IP Address',
				isVisible                       : 'Visible',
				delegatedAdmin					: 'Delegate Admin',
				policyId						: 'Policy ID',
				moduleName						: 'Module Name',
				keyManagement					: 'Key Management',
				addNewKey						: 'Add New Key',
				keyName							: 'Key Name',
				cipher							: 'Cipher',
				length							: 'Length',
				version							: 'Version',
				attributes						: 'Attributes',
				material						: 'Material',
				addNewConfig					: 'Add New Configurations',
				createService					: 'Create Service',
				editService						: 'Edit Service',
				serviceDetails					: 'Service Details',
				serviceName						: 'Service Name',
				PolicyType_ALLOW				: 'Allow',
				PolicyType_DENY					: 'Deny',
				componentPermissions			: 'Component Permissions',
				selectDataMaskTypes				: 'Select Data Mask Types',
				accessTypes						: 'Access Types',
				rowLevelFilter					: 'Row Level Filter',
				selectMaskingOption				: 'Select Masking Option'
			},
			btn : {
				add							: 'Add',
				save						: 'Save',
				cancel 						: 'Cancel',
				addMore						: 'Add More..',
				stayOnPage					: 'Stay on this page',
				leavePage					: 'Leave this page',
				setVisibility               : 'Set Visibility',
				setStatus               	: 'Set Status'
				
			},
			// h1, h2, h3, fieldset, title
			h : {
				welcome						: 'Welcome',
				logout 						: 'Logout',
				xaSecure					: 'XA Secure',
				listOfPlugins				: 'See third-party tools/resources that Ranger uses and their respective authors.',
				licenseText                 : 'Licensed under the Apache License, Version 2.0',
				
	
				// Menu
				dashboard					: 'Dashboard',
				policyManager 				: 'Policy Manager',
				usersOrGroups 				: 'Users/Groups',
				reports 					: 'Reports',
				config 						: 'Config',
				accounts					: 'Accounts',
				analytics					: 'Analytics',
				audit						: 'Audit',
				repositoryManager			: 'Manage Repository',
				serviceManager				: 'Service Manager',
				hdfs  						: 'HDFS',
				hive  						: 'Hive',
				createPolicy 				: 'Create Policy',
				editPolicy	 				: 'Edit Policy',
				managePolices 				: 'Manage Polices',
				manageTables				: 'Manage Table',
				userProfile					: 'User Profile',
				users						: 'Users',
				agents						: 'Plugins',
				repository					: 'Repository',
				policy						: 'Policy',
				policyDetails				: 'Policy Details',
				userGroupPermissions		: 'User and Group Permissions',
				groups						: 'Groups',
				admin						: 'Admin',
				bigData						: 'Big Data',
				loginSession				: 'Login Sessions',
				operationDiff				: 'Operation ',
				searchForYourAccessAudit 	:"Search for your access audits...",
				searchForYourAccessLog 		:"Search for your access logs...",
				searchForYourLoginSession 	:"Search for your login sessions...",
				searchForYourAgent 			:"Search for your plugins...",
				searchForPolicy				:"Search for your policy...",
				searchForPermissions		:"Search for permissions...",
				searchForYourUser 			:"Search for your users...",
				searchForYourGroup 			:"Search for your groups...",
				access						: 'Access',
				policyCondition				: 'Policy Conditions',
				permissions					: 'Permissions',
				kms							: 'KMS',
				keyCreate					: 'Key Create',
				keyEdit						: 'Key Edit',
				searchForKeys				:"Search for your keys...",
				encryption					: 'Encryption',
				settings					: 'Settings',
				
				
			},
			msg : {
				deletePolicyValidationMsg : 'Policy does not have any settings for the specific resource. Policy will be deleted. Press [Ok] to continue. Press [Cancel] to edit the policy.',
				areYouSureWantToDelete	  : 'Are you sure want to delete ?',
				policyDeleteMsg 		  : 'Policy deleted successfully',
				policyNotAddedMsg		  : 'Policy not added!',
				addGroupPermission		  : 'Please add permission(s) for the selected Group, else group will not be added.',
				addGroup		  		  : 'Please select group for the selected permission(s), else group will not be added.',
				addUserPermission		  : 'Please add permission(s) for the selected User, else User will not be added.',
				addUser		  		 	  : 'Please select User for the selected permission(s), else User will not be added.',
				enterAlteastOneCharactere : 'Enter alteast one character.',
				permsAlreadyExistForSelectedUser : 'Permission already exists for selected user.',
				permsAlreadyExistForSelectedGroup : 'Permission already exists for selected group.',
				youDontHavePermission 	  : 'You don\'t have permission for the resource !!',
				myProfileError			  :'Your password does not match. Please try again with proper password',
				myProfileSuccess		  :'Profile Edited successfully',
				userNameAlreadyExist	  : 'User name already exists',
				groupNameAlreadyExist	  : 'Group name already exists',
				yourAuditLogginIsOff 	  :'You must have at least one or more user/group access defined for the policy.',
				policyNotHavingPerm		  : 'The policy does not have any permissions so audit logging cannot be turned off',
				areSureWantToLogout		  : 'Are you sure want to logout ?',
				groupDoesNotExistAnymore  : 'Group does not exist anymore..',
				userDoesNotExistAnymore   : 'User does not exist anymore..',
				repoDoesNotExistAnymore   : 'Repository does not exist anymore..',
				policyDisabledMsg		  : 'This policy is currently in disabled state.',
				noRecordsFound			  : 'No Records Found',
				keyDeleteMsg			  : 'Key deleted successfully',
				rolloverSuccessfully	  : 'Key rollover successfully',
				addUserOrGroup			  : 'Please select group/user for the selected permission, else group/user will not be added.',
				addUserOrGroupForPC		  : 'Please select group/user for the added policy condition, else group/user will not be added.',
				userCreatedSucc		      : 'User created successfully',
				userUpdatedSucc           :     'User updated successfully',
				grpUpdatedSucc            : 'Group updated successfully',
				grpCreatedSucc            : 'Group created successfully',
				errorLoadingAuditLogs	  : 'Unable to connect to Audit store !!',
				enterCustomMask			  : 'Please enter custom masked value or expression !!'
				
				
				
			},
			plcHldr : {
				search 						:'Search',
				searchByResourcePath		:'Search by resource path'
			},
			dialogMsg :{
				preventNavPolicyForm : 'Policy form edit is in progress. Please save/cancel changes before navigating away!',
				preventNavRepositoryForm : 'Repository form edit is in progress. Please save/cancel changes before navigating away!',
				preventNavUserForm : 'User form edit is in progress. Please save/cancel changes before navigating away!',
				preventNavGroupForm : 'Group form edit is in progress. Please save/cancel changes before navigating away!',
				preventNavUserList : 'Some Users/Groups have been edited. Kindly save your changes before navigating away!'
				
			},	
			validationMessages : {
				required 					: "* This field is required",
				onlyLetterNumberUnderscore :'* Only Alpha Numeric and underscore characters are allowed',
				alphaNumericUnderscoreDotComma :'* Only Alpha Numeric,underscore,comma,hypen,dot and space characters are allowed',
				oldPasswordError :'Your password does not match. Please try again with proper password',
				oldPasswordRepeatError :'You can not use old password.',
				newPasswordError :'Invalid Password.Minimum 8 characters with min one alphabet and one numeric.',
				emailIdError				: 'Please enter valid email address.',
				enterValidName				: 'Please enter valid name.',
				passwordError	            :'Invalid Password.Minimum 8 characters with min one alphabet and one numeric.'
			},
			serverMsg : {
				
				// UserMgr
				userMgrPassword : 'The password you\'ve provided is incorrect. Please try again with correct password',
				userMgrInvalidUser : 'Invalid user provided',
				userMgrNewPassword : 'Invalid new password',
				userMgrOldPassword : ' You can not use old password.',
				userMgrEmailChange : 'Email address cannot be changed. Please send a request to change using feedback',
				userMgrInvalidEmail : 'Invalid email address',
				userMgrWrongPassword : 'Password doesnot match. Please try again with proper password',
				userMgrWrongUser : 'User access denied. User not found.',
				fsDefaultNameValidationError:"Please provide  fs.default.name in  format 'hdfs://hostname:portNumber' .",
				fsDefaultNameEmptyError:'Please provide  fs.default.name.',
				userAlreadyExistsError : 'User already exists',
				repositoryNameAlreadyExistsError  : 'Repository name already exists'
				
			}
			

        }
    });
});
