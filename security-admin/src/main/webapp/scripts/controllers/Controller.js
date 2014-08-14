/*
 * Copyright (c) 2014 XASecure
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure.
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
		
	   //************** Policy Related *********************/
	   policyManagerAction :function(){
		   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   console.log('Policy Manager action called..');
		   var view 		= require('views/policymanager/PolicyManagerLayout');
		   var VXAssetList 	= require('collections/VXAssetList');
		   var collection 	= new VXAssetList();
		   
		   collection.fetch({
			   cache : false,
			   async:false
		   }).done(function(){
			   if(App.rContent.currentView) App.rContent.currentView.close();
			   App.rContent.show(new view({
				   collection : collection
			   }));
		   });
	   },
	   
	   hdfsManageAction :function(assetId){
		   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   var view 			= require('views/hdfs/HDFSTableLayout');
		   var VXResourceList 	= require('collections/VXResourceList');
		   var VXAsset 		  	= require('models/VXAsset');
		   var resourceListForAsset = new VXResourceList([],{
			   queryParams : {
				   'assetId' : assetId 
			   }
		   });
		   var assetModel = new VXAsset({id:assetId});
		   assetModel.fetch({cache : true}).done(function(){
			   App.rContent.show(new view({
				   collection : resourceListForAsset,
				   assetModel : assetModel
			   }));
			   resourceListForAsset.fetch({
				   cache : true
			   });
		   });   
	   },

	   policyCreateAction :function(assetId){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });

		   var view 			= require('views/policy/PolicyCreate');
		   var VXResource 		= require('models/VXResource');
		   var VXResourceList 	= require('collections/VXResourceList');
		   var VXAsset 	  		= require('models/VXAsset');
		   
		   var assetModel 	= new VXAsset({id:assetId});
		   var resource		= new VXResource();
		   resource.collection = new VXResourceList([],{
			   queryParams : {
				   'assetId' : assetId 
			   }
		   });
		   
		   assetModel.fetch({cache : true}).done(function(){
			   App.rContent.show(new view({
				   model : resource,
				   assetModel : assetModel
			   }));
		   });
	   },
	   
	   policyEditAction :function(){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   var view = require('views/policy/PolicyCreate');
		   App.rContent.show(new view({
		   }));
	   },

	   policyViewAction :function(assetId,id){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });

		   var VXResource 	  = require('models/VXResource');
		   var VXResourceList = require('collections/VXResourceList');
		   var VXAsset 	  	  = require('models/VXAsset');
		   var view = require('views/policy/PolicyCreate');
		   
		   var resource = new VXResource({id : id });
		   resource.collection = new VXResourceList([],{
			   queryParams : {
				   'assetId' : assetId 
			   }
		   });
		   
		   var assetModel = new VXAsset({id:assetId});
		   resource.fetch({cache:true}).done(function(){
			   assetModel.fetch({cache : true}).done(function(){
				   App.rContent.show(new view({
					   model : resource,
					   assetModel : assetModel
				   }));
			   });
		   });
	   },
	   //************** Policy Related ( HIVE )*********************/
	   hiveManageAction :function(assetId){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   var view 			= require('views/hive/HiveTableLayout');
		   var VXResourceList 	= require('collections/VXResourceList');
		   var VXAsset 		  	= require('models/VXAsset');
		   var resourceListForAsset = new VXResourceList([],{
			   queryParams : {
				   'assetId' : assetId 
			   }
		   });
		   var assetModel = new VXAsset({id:assetId});
		   assetModel.fetch({cache : true}).done(function(){
			   App.rContent.show(new view({
				   collection : resourceListForAsset,
				   assetModel : assetModel
			   }));
			   resourceListForAsset.fetch({
				   cache : true
			   });
		   });   
	   },
	   hivePolicyCreateAction :function(assetId){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   var view 			= require('views/hive/HivePolicyCreate');
		   var VXResource 		= require('models/VXResource');
		   var VXResourceList 	= require('collections/VXResourceList');
		   var VXAsset 		  	= require('models/VXAsset');
		   
		   var assetModel = new VXAsset({id:assetId});
		   var resource	  = new VXResource();
		   resource.collection = new VXResourceList([],{
			   queryParams : {
				   'assetId' : assetId 
			   }
		   });
		   assetModel.fetch({cache : true}).done(function(){
			   App.rContent.show(new view({
				   model : resource,
			   		assetModel : assetModel
			   }));
		   });	   
	   },
	   hivePolicyEditAction :function(assetId,id){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
    	   var VXResource 		= require('models/VXResource');
    	   var VXResourceList 	= require('collections/VXResourceList');
    	   var VXAsset 		  	= require('models/VXAsset');
		   var view 			= require('views/hive/HivePolicyCreate');
		   var resource 		= new VXResource({id : id });
		   resource.collection = new VXResourceList([],{
			   queryParams : {
				   'assetId' : assetId 
			   }
		   });
		   var assetModel = new VXAsset({id:assetId});
		   resource.fetch({cache:true}).done(function(){
			   assetModel.fetch({cache : true}).done(function(){
				   App.rContent.show(new view({
					   model : resource,
					   assetModel : assetModel
				   }));
			   });
		   });
	   },
	   
	   //************** Policy Related ( KNOX )*********************/
	   knoxManageAction :function(assetId){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   var view 			= require('views/knox/KnoxTableLayout');
		   var VXResourceList 	= require('collections/VXResourceList');
		   var VXAsset 		  	= require('models/VXAsset');
		   
		   var resourceListForAsset = new VXResourceList([],{
			   queryParams : {
				   'assetId' : assetId 
			   }
		   });
		   var assetModel = new VXAsset({id:assetId});
		   assetModel.fetch({cache : true}).done(function(){
			   App.rContent.show(new view({
				   collection  :resourceListForAsset,
				   assetModel : assetModel
			   }));
			   resourceListForAsset.fetch({
				   cache : true
			   });
		   });   
	   },
	   knoxPolicyCreateAction :function(assetId){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   var view 			= require('views/knox/KnoxPolicyCreate');
		   var VXResource 		= require('models/VXResource');
		   var VXResourceList 	= require('collections/VXResourceList');
		   var VXAsset 		  	= require('models/VXAsset');
		   
		   var assetModel = new VXAsset({id:assetId});
		   var resource	  = new VXResource();
		   resource.collection = new VXResourceList([],{
			   queryParams : {
				   'assetId' : assetId 
			   }
		   });
		   assetModel.fetch({cache : true}).done(function(){
			   App.rContent.show(new view({
				   model : resource,
			   		assetModel : assetModel
			   }));
		   });	   
	   },
	   knoxPolicyEditAction :function(assetId,id){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
    	   var VXResource 		= require('models/VXResource');
    	   var VXResourceList 	= require('collections/VXResourceList');
    	   var VXAsset 		  	= require('models/VXAsset');
		   var view 			= require('views/knox/KnoxPolicyCreate');
		   var resource 		= new VXResource({id : id });
		   resource.collection = new VXResourceList([],{
			   queryParams : {
				   'assetId' : assetId 
			   }
		   });
		   var assetModel = new VXAsset({id:assetId});
		   resource.fetch({cache:true}).done(function(){
			   assetModel.fetch({cache : true}).done(function(){
				   App.rContent.show(new view({
					   model : resource,
					   assetModel : assetModel
				   }));
			   });
		   });
	   },
	 //************** Policy Related ( STORM )*********************/
	   stormManageAction :function(assetId){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   var view 			= require('views/storm/StormTableLayout');
		   var VXResourceList 	= require('collections/VXResourceList');
		   var VXAsset 		  	= require('models/VXAsset');
		   
		   var resourceListForAsset = new VXResourceList([],{
			   queryParams : {
				   'assetId' : assetId 
			   }
		   });
		   var assetModel = new VXAsset({id:assetId});
		   assetModel.fetch({cache : true}).done(function(){
			   App.rContent.show(new view({
				   collection  :resourceListForAsset,
				   assetModel : assetModel
			   }));
			   resourceListForAsset.fetch({
				   cache : true
			   });
		   });   
	   },
	   stormPolicyCreateAction :function(assetId){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   var view 			= require('views/storm/StormPolicyCreate');
		   var VXResource 		= require('models/VXResource');
		   var VXResourceList 	= require('collections/VXResourceList');
		   var VXAsset 		  	= require('models/VXAsset');
		   
		   var assetModel = new VXAsset({id:assetId});
		   var resource	  = new VXResource();
		   resource.collection = new VXResourceList([],{
			   queryParams : {
				   'assetId' : assetId 
			   }
		   });
		   assetModel.fetch({cache : true}).done(function(){
			   App.rContent.show(new view({
				   model : resource,
			   		assetModel : assetModel
			   }));
		   });	   
	   },
	   stormPolicyEditAction :function(assetId,id){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
    	   var VXResource 		= require('models/VXResource');
    	   var VXResourceList 	= require('collections/VXResourceList');
    	   var VXAsset 		  	= require('models/VXAsset');
		   var view 			= require('views/storm/StormPolicyCreate');
		   var resource 		= new VXResource({id : id });
		   resource.collection = new VXResourceList([],{
			   queryParams : {
				   'assetId' : assetId 
			   }
		   });
		   var assetModel = new VXAsset({id:assetId});
		   resource.fetch({cache:true}).done(function(){
			   assetModel.fetch({cache : true}).done(function(){
				   App.rContent.show(new view({
					   model : resource,
					   assetModel : assetModel
				   }));
			   });
		   });
	   },
	   //************** Asset Related ( Repository )*********************/
	   assetCreateAction :function(assetType){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   var view		= require('views/asset/AssetCreate');
		   var VXAsset	= require('models/VXAsset');
		   App.rContent.show(new view({
			   model : new VXAsset().set('assetType',assetType)
		   }));
	   },

	   assetEditAction :function(id){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   var view		= require('views/asset/AssetCreate');
		   var VXAsset	= require('models/VXAsset');

		   var model = new VXAsset({id : id });
		   model.fetch({cache:true}).done(function(){
			   App.rContent.show(new view({
				   model : model
			   }));
		   });
	   },
 //************** Policy Related ( HBASE)*********************/
	   hbaseManageAction :function(assetId){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   var view 			= require('views/hbase/HbaseTableLayout');
		   var VXResourceList 	= require('collections/VXResourceList');
		   var VXAsset 		  	= require('models/VXAsset');
		   var resourceListForAsset = new VXResourceList([],{
			   queryParams : {
				   'assetId' : assetId 
			   }
		   });
		   var assetModel = new VXAsset({id:assetId});
		   assetModel.fetch({cache:true}).done(function(){
			   App.rContent.show(new view({
				   collection : resourceListForAsset,
				   assetModel : assetModel
			   }));
			   resourceListForAsset.fetch({
				   cache : true
			   });
		   });   
		   
	   },
	   hbasePolicyCreateAction :function(assetId){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
		   var view 			= require('views/hbase/HbasePolicyCreate');
		   var VXResource 		= require('models/VXResource');
		   var VXResourceList 	= require('collections/VXResourceList');
		   var VXAsset 		  	= require('models/VXAsset');
		   var resource	  		= new VXResource();
		   resource.collection  = new VXResourceList([],{
			   queryParams : {
				   'assetId' : assetId 
			   }
		   });
		   var assetModel = new VXAsset({id:assetId});
		   assetModel.fetch({cache:true}).done(function(){
			   App.rContent.show(new view({
				   model : resource,
			   		assetModel : assetModel
			   }));
		   }); 
	   },
	   hbasePolicyEditAction :function(assetId,id){
    	   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.PolicyManager.value });
    	   var VXResource 		= require('models/VXResource');
    	   var VXResourceList 	= require('collections/VXResourceList');
    	   var VXAsset 		  	= require('models/VXAsset');
		   var view 			= require('views/hbase/HbasePolicyCreate');
		   var resource 		= new VXResource({id : id });
		   var assetModel 		= new VXAsset({id:assetId});

		   resource.collection = new VXResourceList([],{
			   queryParams : {
				   'assetId' : assetId 
			   }
		   });
		   resource.fetch({cache:true}).done(function(){
			   assetModel.fetch({cache:true}).done(function(){
				   App.rContent.show(new view({
					   model : resource,
					   assetModel : assetModel
				   }));
			   });
		   });
	   },

	   //************** Analytics(reports)  Related *********************/
	   userAccessReportAction : function(){
		   MAppState.set({ 'currentTab' : XAGlobals.AppTabs.Analytics.value });
		   var view				= require('views/reports/UserAccessLayout');
		   var VXResourceList 	= require('collections/VXResourceList');
		   var VXGroupList		= require('collections/VXGroupList');
		   var VXUserList		= require('collections/VXUserList');
		   var resourceList 	= new VXResourceList([],{
			   queryParams : {
				   //'resourceType' : XAEnums.AssetType.ASSET_HDFS.value,
				   //'assetId' : assetId 
			   }
		   });
		   var that 		= this;
		   this.groupList 	= new VXGroupList();
		   this.userList 	= new VXUserList();
		   resourceList.setPageSize(200, {fetch : false});
		   resourceList.fetch({
			   async:false,
			   cache : false
		   }).done(function(){
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
							collection : resourceList,
							groupList :that.groupList,
							userList :that.userList
						}));
					});
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
		   var VXGroupList	= require('collections/VXGroupList');

		   var groupList = new VXGroupList();
		   var user 	 = new VXUser({id : userId});
		   
		   user.collection = new VXUserList();
		   user.fetch({cache : true}).done(function(){
			   	   groupList.fetch({cache : true ,async:false}).done(function(){
				   App.rContent.show(new view({
					   model : user,
					   groupList :groupList
				   }));
			   });   
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
	   }
	});
});
