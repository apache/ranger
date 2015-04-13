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

 
define(function(require) {
    'use strict';
	var Backbone	= require('backbone');
	var App			= require('App');
	
	var MAppState	= require('models/VAppState');
	var XAGlobals	= require('utils/XAGlobals');

	return Backbone.Marionette.Controller.extend({

		initialize: function( options ) {

			console.log("initialize a Controller Controller");
			var vTopNav 	= require('views/common/TopNav');
			var vProfileBar = require('views/common/ProfileBar');
			var vFooter 	= require('views/common/Footer');

			App.rTopNav.show(new vTopNav({
				model : App.userProfile,
				appState : MAppState
			}));

			App.rTopProfileBar.show(new vProfileBar({}));

			App.rFooter.show(new vFooter({}));
		},

		dashboardAction: function (action) {
            console.log('dashboard action called..');
			var vDashboardLayout	= require('views/common/DashboardLayout');
            MAppState.set({
				'currentTab' : XAGlobals.AppTabs.Dashboard.value
			});
			
            App.rContent.show(new vDashboardLayout({}));
        },
	   
	   //************** Analytics(reports)  Related *********************/
	   userAccessReportAction : function(){
		   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.Analytics.value });
		   var view				= require('views/reports/UserAccessLayout');
		   var RangerPolicyList 	= require('collections/RangerPolicyList');
		   var VXGroupList		= require('collections/VXGroupList');
		   var VXUserList		= require('collections/VXUserList');
		   var policyList 	= new RangerPolicyList([],{
			   queryParams : {
				   //'resourceType' : XAEnums.AssetType.ASSET_HDFS.value,
				   //'assetId' : assetId 
			   }
		   });
		   var that 		= this;
		   this.groupList 	= new VXGroupList();
		   this.userList 	= new VXUserList();
		   that.groupList.fetch({
					async:false,
					cache:false
				}).done(function(){
				that.userList.fetch({
					async:false,
					cache:false
				}).done(function(){
					if(App.rContent.currentView)
						   App.rContent.currentView.close();
					App.rContent.show(new view({
						collection : policyList,
						groupList :that.groupList,
						userList :that.userList
					}));
				});
			});
	   },
	   auditReportAction : function(tab){
		   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.Audit.value });
		   var view					= require('views/reports/AuditLayout');
		   var VXAccessAuditList 	= require('collections/VXAccessAuditList');
		   var accessAuditList 		= new VXAccessAuditList();
		   App.rContent.show(new view({
			   accessAuditList : accessAuditList,
			   tab :tab
		   }));
		   if(tab == 'bigData'){
			  accessAuditList.fetch({
				 cache : false,
				 async:true
			   });
		   }
	   },
	   loginSessionDetail : function(type, id){
		   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.Audit.value });
		   var view					= require('views/reports/LoginSessionDetail');
		   var VXAuthSessionList	= require('collections/VXAuthSessionList');
		   var authSessionList 		= new VXAuthSessionList();
		   authSessionList.fetch({
			   data : {id : id}
		   }).done(function(){
			   App.rContent.show(new view({
				   model : authSessionList.first()
			   }));
		   });
	   },
	   //************** UserProfile Related *********************/
	   userProfileAction : function(){
		   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.None.value });
		   var view				= require('views/user/UserProfile');
		   
		   App.rContent.show(new view({
			   model : App.userProfile
		   }));

	   },
	   
	   /************** UserORGroups Related *********************/
	   userManagerAction :function(tab){
		   MAppState.set({
				'currentTab' : XAGlobals.AppTabs.Users.value
			});
		   var view 		= require('views/users/UserTableLayout');
		   var VXUserList	= require('collections/VXUserList');
		   var userList 	= new VXUserList();
		   
		   App.rContent.show(new view({
			   collection : userList,
			   tab :tab
		   }));
		   userList.fetch({
			   cache:true
		   });
	   },
	   userCreateAction : function(){
		   MAppState.set({
				'currentTab' : XAGlobals.AppTabs.Users.value
			});
		   var view 		= require('views/users/UserCreate');
		   var VXUser		= require('models/VXUser');
		   var VXUserList	= require('collections/VXUserList');
		   var VXGroupList	= require('collections/VXGroupList');

		   var groupList = new VXGroupList();
		   var user 	 = new VXUser();
		   user.collection = new VXUserList();
	   	   groupList.fetch({cache:true,async:false}).done(function(){
			   App.rContent.show(new view({
				   model : user,
				   groupList :groupList
			   }));
		   });   
	   },
	   userEditAction : function(userId){
		   MAppState.set({
				'currentTab' : XAGlobals.AppTabs.Users.value
			});
		   var view 		= require('views/users/UserCreate');
		   var VXUser		= require('models/VXUser');
		   var VXUserList	= require('collections/VXUserList');

		   var user 	 = new VXUser({id : userId});
		   
		   user.collection = new VXUserList();
		   user.fetch({cache : true}).done(function(){
			   App.rContent.show(new view({
				   model : user,
			   }));
		   });
	   },
	   groupCreateAction : function(){
		   MAppState.set({
				'currentTab' : XAGlobals.AppTabs.Users.value
			});
		   var view 		= require('views/users/GroupCreate');
		   var VXGroup		= require('models/VXGroup');
		   var VXGroupList	= require('collections/VXGroupList');
		   
		   var group 		= new VXGroup();
		   group.collection = new VXGroupList();
		   App.rContent.show(new view({
			   model : group
		   }));
	   },
	   groupEditAction : function(groupId){
		   MAppState.set({
				'currentTab' : XAGlobals.AppTabs.Users.value
			});
		   var view 		= require('views/users/GroupCreate');
		   var VXGroup		= require('models/VXGroup');
		   var VXGroupList	= require('collections/VXGroupList');
		   
		   var group 		= new VXGroup({id : groupId});
		   group.collection = new VXGroupList();
		   
		   group.fetch({cache : true}).done(function(){
			   App.rContent.show(new view({
				   model : group
			   }));
		   });	   
	   },


   	   /************************************************************/
   	   //************** Generic design Related *********************/
   	   /************************************************************/

	   serviceManagerAction :function(){
		   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   console.log('Policy Manager action called..');
		   var view 				= require('views/policymanager/ServiceLayout');
		   var RangerServiceDefList	= require('collections/RangerServiceDefList');
		   var collection 			= new RangerServiceDefList();
		   collection.queryParams.sortBy = 'serviceTypeId';
		   collection.fetch({
			   cache : false,
			   async:false
		   }).done(function(){
			   if(App.rContent.currentView) App.rContent.currentView.close();
			   //TODO FROM SERVER SIDE IT SHOULD GIVE SORTBY `ID` BY DEFAULT
//			   collection = collection.sort()
			   App.rContent.show(new view({
				   collection : collection
			   }));
		   });
	   },

	   serviceCreateAction :function(serviceTypeId){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   var view				= require('views/service/ServiceCreate');
		   var RangerServiceDef	= require('models/RangerServiceDef');
		   var RangerService	= require('models/RangerService');
		   
		   var rangerServiceDefModel	= new RangerServiceDef({id:serviceTypeId});
		   var rangerServiceModel 	= new RangerService();

		   App.rContent.show(new view({
			   model 		: rangerServiceModel,
			   serviceTypeId : serviceTypeId
		   }));
	   },
	   serviceEditAction :function(serviceTypeId, serviceId){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   var view				= require('views/service/ServiceCreate');
		   var RangerServiceDef	= require('models/RangerServiceDef');
		   var RangerService	= require('models/RangerService');
		   
		   var rangerServiceDefModel	= new RangerServiceDef({ id : serviceTypeId });
		   var rangerService = new RangerService({ 'id' : serviceId });

		   rangerService.fetch({
			   cache:false
		   }).done(function(){
			   App.rContent.show(new view({
				   model 			: rangerService,
				   serviceTypeId	:serviceTypeId
			   }));
		   });
	   },
	   
	   policyManageAction :function(serviceId){
		   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   var XAUtil			= require('utils/XAUtils');
		   var view 			= require('views/policies/RangerPolicyTableLayout');
		   var RangerService	= require('models/RangerService');
		   var RangerPolicyList	= require('collections/RangerPolicyList');
		   
		   var rangerService = new RangerService({id : serviceId});
		   var rangerPolicyList = new RangerPolicyList();
		   rangerPolicyList.url = XAUtil.getServicePoliciesURL(serviceId);
		   
		   rangerService.fetch({
			  cache : false,
			  async : false
		   });
		   rangerPolicyList.fetch({
			   cache : false,
		   });
		   App.rContent.show(new view({
			   collection : rangerPolicyList,
			   rangerService : rangerService
		   }));
	   },
	   RangerPolicyCreateAction :function(serviceId){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });

		   var view 			= require('views/policies/RangerPolicyCreate');
		   var RangerService	= require('models/RangerService');
		   var RangerPolicy		= require('models/RangerPolicy');
		   
		   var rangerService = new RangerService({id : serviceId});
		   rangerService.fetch({
				  cache : false,
		   }).done(function(){
			   App.rContent.show(new view({
				   model : new RangerPolicy(),
				   rangerService : rangerService,
			   }));
		   });
	   },
	   RangerPolicyEditAction :function(serviceId, policyId){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });

		   var view 			= require('views/policies/RangerPolicyCreate');
		   var RangerService	= require('models/RangerService');
		   var RangerPolicy		= require('models/RangerPolicy');
		   
		   var rangerService = new RangerService({id : serviceId});
		   var rangerPolicy = new RangerPolicy({ id : policyId});
		   rangerService.fetch({
			   cache : false,
			   async : false,
		   });
		   rangerPolicy.fetch({
				  cache : false,
		   }).done(function(){
			   App.rContent.show(new view({
				   model : rangerPolicy,
				   rangerService :rangerService
			   }));
		   });
	   },
	   /************PERMISSIONS LISTING *****************************************/
	   modulePermissionsAction :function(){
		   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.Permissions.value });
		   var view 			= require('views/permissions/ModulePermsTableLayout');
		   var ModulePermission	= require('models/VXModuleDef');
		   var ModulePermissionList	= require('collections/VXModuleDefList');

		   var modulePermission = new ModulePermission();
		   var modulePermissionList = new ModulePermissionList();

		   modulePermissionList.fetch({
			   cache : false,
		   });
		   App.rContent.show(new view({
			   collection : modulePermissionList
		   }));

	   },
	   modulePermissionEditAction : function(moduleId){
		   MAppState.set({
				'currentTab' : XAGlobals.AppTabs.Permissions.value
			});
		   var view 				= require('views/permissions/ModulePermissionCreate');
		   var ModulePermission		= require('models/VXModuleDef');
		   var ModulePermissionList	= require('collections/VXModuleDefList');
		   var modulePermission 	= new ModulePermission({id : moduleId});
		   var that = this
		   modulePermission.collection = new ModulePermissionList();
		   modulePermission.fetch({cache : true}).done(function(){
			   App.rContent.show(new view({
				   model : modulePermission,
				   groupList : that.groupList,
				   userList : that.userList
			   }));
		   });
	   },
	   postLoginDefaultView : function(){
                var SessionMgr  = require('mgrs/SessionMgr');
                var XAGlobals   = require('utils/XAGlobals');
                var XALinks             = require('modules/XALinks');
                var vXPortalUser = SessionMgr.getUserProfile();
                var userModuleNames = _.pluck(vXPortalUser.get('userPermList'),'moduleName');
                if (_.contains(userModuleNames, 'Policy Manager')){
                        location.hash = XALinks.get('ServiceManager').href;
                }else if(_.contains(userModuleNames, 'Users/Groups')){
                        location.hash = XALinks.get('Users').href;
                }else if(_.contains(userModuleNames, 'Analytics')){
                        location.hash = XALinks.get('UserAccessReport').href;
                }else if(_.contains(userModuleNames, 'Audit')){
                        location.hash = XALinks.get('AuditReport').href +'/bigData';
		}else if(SessionMgr.isSystemAdmin()){
			location.hash = XALinks.get('ModulePermissions').href;
		}else{
			//If a user doesnot has access to any tab - taking user to by default Profile page.
			location.hash = XALinks.get('UserProfile').href;
		}
           },
	   /**************** ERROR PAGE ******************************/
	   pageNotFoundAction	: function() {
		   var XAUtils			= require('utils/XAUtils');
		   XAUtils.defaultErrorHandler(undefined, { 'status' : 404 });
	   },
	});
});
