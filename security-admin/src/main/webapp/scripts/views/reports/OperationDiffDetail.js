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

	var Backbone						= require('backbone');
	var XAEnums					 		= require('utils/XAEnums');
	var XALinks							= require('modules/XALinks');
	var PolicyOperationDiff_tmpl 		= require('hbs!tmpl/reports/PolicyOperationDiff_tmpl');
	var PolicyUpdateOperationDiff_tmpl 	= require('hbs!tmpl/reports/PolicyUpdateOperationDiff_tmpl');
	var PolicyDeleteUpdateOperationDiff_tmpl 	= require('hbs!tmpl/reports/PolicyDeleteOperationDiff_tmpl');
	var KnoxPolicyOperationDiff_tmpl 			= require('hbs!tmpl/reports/KnoxPolicyOperationDiff_tmpl');
	var KnoxPolicyUpdateOperationDiff_tmpl 		= require('hbs!tmpl/reports/KnoxPolicyUpdateOperationDiff_tmpl');
	var KnoxPolicyDeleteUpdateOperationDiff_tmpl= require('hbs!tmpl/reports/KnoxPolicyDeleteOperationDiff_tmpl');
	var AssetOperationDiff_tmpl 		= require('hbs!tmpl/reports/AssetOperationDiff_tmpl');
	var AssetUpdateOperationDiff_tmpl 	= require('hbs!tmpl/reports/AssetUpdateOperationDiff_tmpl');
	var UserOperationDiff_tmpl 			= require('hbs!tmpl/reports/UserOperationDiff_tmpl');
	var UserUpdateOperationDiff_tmpl 	= require('hbs!tmpl/reports/UserUpdateOperationDiff_tmpl');
	var GroupOperationDiff_tmpl 		= require('hbs!tmpl/reports/GroupOperationDiff_tmpl');
	var GroupUpdateOperationDiff_tmpl 	= require('hbs!tmpl/reports/GroupUpdateOperationDiff_tmpl');
	
	var OperationDiffDetail = Backbone.Marionette.ItemView.extend(
	/** @lends OperationDiffDetail */
	{
		_viewName : 'OperationDiffDetail',
		
    	template: PolicyOperationDiff_tmpl,
        templateHelpers :function(){
        	var obj = {
        			collection : this.collection.models,
        			action	   : this.action,
        			objectName : this.objectName,
        			objectId   : this.objectId,
        			objectCreatedDate : this.objectCreatedDate,
        			userName   : this.userName
        		};
        	if(this.templateType == XAEnums.ClassTypes.CLASS_TYPE_XA_ASSET.value){
        		obj = $.extend(obj, {
        			newConnConfig 		: this.newConnConfig,
        			previousConnConfig 	: this.previousConnConfig,
        			isNewConnConfig		: _.isEmpty(this.newConnConfig) ? false : true,
   					isPreviousConnConfig: _.isEmpty(this.previousConnConfig) ? false : true
        		});
        	}
        	if(this.templateType == XAEnums.ClassTypes.CLASS_TYPE_XA_RESOURCE.value){
        		obj = $.extend(obj, {newGroupPermList 			: this.newGroupPermList, 
				        			previousGroupPermList 		: this.previousGroupPermList,
        							newUserPermList 			: this.newUserPermList,
        							previousUserPermList 		: this.previousUserPermList,
        							isGroupPerm 				: this.isGroupPerm,
        							isUserPerm 					: this.isUserPerm,
        							groupList					: this.groupList,
        							userList					: this.userList,
        							repositoryType				: this.repositoryType,
        							policyName					: this.policyName
        			  });
        	}
        	if(this.templateType == XAEnums.ClassTypes.CLASS_TYPE_XA_USER.value){
        		obj = $.extend(obj, { 
        				newGroupList 		: this.newGroupList,
        				previousGroupList 	: this.previousGroupList,
        				isGroup 			: this.isGroup
        		});
        	}
        	
        	
        	return obj;
        },
    	/** ui selector cache */
    	ui: {
    		groupPerm : '.groupPerm',
    		userPerm  : '.userPerm',
    		oldValues : '[data-id="oldValues"]',
    		diff 	  : '[data-id="diff"]',
    		policyDiff: '[data-name="policyDiff"]'
    		
    	},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			return events;
		},

    	/**
		* intialize a new OperationDiffDetail ItemView 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a OperationDiffDetail ItemView");
			
			_.extend(this, _.pick(options, 'classType','objectName','objectId','objectCreatedDate','action','userName'));
			this.bindEvents();
			this.getTemplateForView();
			
		},

		/** all events binding here */
		bindEvents : function(){
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
		},
		/** on render callback */
		onRender: function() {
			this.initializePlugins();
			
			//remove last comma from Perms
			_.each(this.ui.diff.find('ol li'),function(m){
				var text = $(m).text().replace(/,(?=[^,]*$)/, '');
				$(m).find('span').last().remove();
			});
			_.each(this.ui.policyDiff.find('ol li'),function(m){
				if(_.isEmpty($(m).text().trim()))
					$(m).removeClass('change-row').text('--');
			});
		},
		getTemplateForView : function(){
			if(this.classType == XAEnums.ClassTypes.CLASS_TYPE_XA_ASSET.value
					|| this.classType == XAEnums.ClassTypes.CLASS_TYPE_RANGER_SERVICE.value){
				this.templateType=XAEnums.ClassTypes.CLASS_TYPE_XA_ASSET.value;
				if(this.action == 'create'){
					this.template = AssetOperationDiff_tmpl;
				} else if(this.action == 'update'){
					this.template = AssetUpdateOperationDiff_tmpl;
				} else {
					this.template = AssetOperationDiff_tmpl;
				}
				this.assetDiffOperation();
			}
			if(this.classType == XAEnums.ClassTypes.CLASS_TYPE_XA_RESOURCE.value){
				this.templateType=XAEnums.ClassTypes.CLASS_TYPE_XA_RESOURCE.value;
				if(this.action == 'create'){
					this.template = PolicyOperationDiff_tmpl;
				} else if(this.action == 'update'){
					this.template = PolicyUpdateOperationDiff_tmpl;
				} else{
					this.template = PolicyDeleteUpdateOperationDiff_tmpl;
				}
				this.resourceDiffOperation();
			} 
			if(this.classType == XAEnums.ClassTypes.CLASS_TYPE_XA_USER.value
					|| this.classType == XAEnums.ClassTypes.CLASS_TYPE_USER_PROFILE.value
					|| this.classType == XAEnums.ClassTypes.CLASS_TYPE_PASSWORD_CHANGE.value){
				if(this.action == 'create' || this.action == 'delete'){
					this.template = UserOperationDiff_tmpl;	
				} else if(this.action == 'update' || this.action == "password change"){
					this.template = UserUpdateOperationDiff_tmpl;
				} else{
					this.template = UserOperationDiff_tmpl;
				}
				this.userDiffOperation();
				this.templateType = XAEnums.ClassTypes.CLASS_TYPE_XA_USER.value;
			} 
			if(this.classType == XAEnums.ClassTypes.CLASS_TYPE_XA_GROUP.value){
				if(this.action == 'create'){
					this.template = GroupOperationDiff_tmpl;
				} else if(this.action == 'update'){
					this.template = GroupUpdateOperationDiff_tmpl;
				} else{
					this.template = GroupOperationDiff_tmpl;
				}
				this.templateType = XAEnums.ClassTypes.CLASS_TYPE_XA_GROUP.value;
			} 
		},
		assetDiffOperation : function(){
			var that = this, configModel;
			
			this.collection.each(function(m){
				if(m.get('attributeName') == 'Connection Configurations'){
					if(m.get('action') != 'delete')
						that.newConnConfig = $.parseJSON(m.get('newValue'));
					if(m.get('action') == 'update' || m.get('action') == 'delete')
						that.previousConnConfig = $.parseJSON(m.get('previousValue'));
					configModel = m;
				}else if(m.get('attributeName') == "Service Status"){
					 var newVal = m.get('newValue'), oldVal = m.get('previousValue');            
					 if(!_.isUndefined(newVal) && !_.isEmpty(newVal)){
					         m.set('newValue', $.parseJSON(newVal) ? XAEnums.ActiveStatus.STATUS_ENABLED.label 
					                         : XAEnums.ActiveStatus.STATUS_DISABLED.label);
					 }
					 if(!_.isUndefined(oldVal) && !_.isEmpty(oldVal)){
					         m.set('previousValue', $.parseJSON(oldVal) ? XAEnums.ActiveStatus.STATUS_ENABLED.label 
					                         : XAEnums.ActiveStatus.STATUS_DISABLED.label);
					 }
				}
			});
			if(configModel)
				this.collection.remove(configModel);
			if(this.action == 'create' || this.action == 'delete'){
				this.newConnConfig 		= this.removeUnwantedFromObject(this.newConnConfig);
				this.previousConnConfig = this.removeUnwantedFromObject(this.previousConnConfig);
			}else{
				var tmp = this.newConnConfig, tmp1 = {};
				_.each(tmp,function(val, name){ tmp1[name] = ""; });
				_.each(this.previousConnConfig,function(val, name){ tmp1[name]=val; });
				this.previousConnConfig = tmp1;
			}
			
		},
		resourceDiffOperation : function(){
			var that = this, modelColl = [];
			this.newGroupPermList = [],this.previousGroupPermList = [], this.newUserPermList = [],this.previousUserPermList = [], this.isGroupPerm = false, this.isUserPerm = false;
			this.userList = [],this.groupList = [];
			this.collection.each(function(m){
				var attrName = m.get('attributeName'), type = 'permType';
				if(attrName == "IP Address")	type = 'ipAddress';
				if(m.get('attributeName') == 'Permission Type' || m.get('attributeName') == "IP Address"){
					if(m.get('parentObjectClassType') == XAEnums.ClassTypes.CLASS_TYPE_XA_GROUP.value){
						if(m.get('action') != 'delete'){
							if(m.get('action') == 'create'){
								var obj = {groupName : m.get('parentObjectName')};
								obj[type] = ""; 
								that.previousGroupPermList.push(obj);
							}
							obj = {groupName : m.get('parentObjectName')};
							obj[type] = m.get('newValue');
							that.newGroupPermList.push(obj);
						}
						if(m.get('action') == 'delete' || m.get('action') == 'update'){
							obj = {groupName : m.get('parentObjectName')};
							obj[type] = m.get('previousValue');
							that.previousGroupPermList.push(obj);
						}
						if($.inArray(m.get('parentObjectName'),that.groupList) < 0)
							that.groupList.push(m.get('parentObjectName'));
					} else {
						if(m.get('action') != 'delete'){
							if(m.get('action') == 'create'){
								var obj = {userName : m.get('parentObjectName')};
								obj[type] = ""; 
								that.previousUserPermList.push(obj);
							}
							obj = {userName : m.get('parentObjectName')};
							obj[type] = m.get('newValue');
							that.newUserPermList.push(obj);
						}
						if(m.get('action') == 'delete' || m.get('action') == 'update'){
							obj = {userName : m.get('parentObjectName')};
							obj[type] = m.get('previousValue');
							that.previousUserPermList.push(obj);
						}
						
						
						if($.inArray(m.get('parentObjectName'),that.userList) < 0)
							that.userList.push(m.get('parentObjectName'));
					}
					modelColl.push(m);
					
				} else if(m.get('attributeName') == 'Repository Type'){
					if(m.get('action') != 'delete'){
						that.repositoryType = m.get('newValue');
					} else {
						that.repositoryType = m.get('previousValue');
					}
					modelColl.push(m);
					if(that.repositoryType == XAEnums.AssetType.ASSET_KNOX.label && m.get('action') == "create")//XAEnums.AssetType.ASSET_KNOX.value)
						that.template = KnoxPolicyOperationDiff_tmpl;
					if(that.repositoryType == XAEnums.AssetType.ASSET_KNOX.label && m.get('action') == "update")
						that.template = KnoxPolicyUpdateOperationDiff_tmpl;
					if(that.repositoryType == XAEnums.AssetType.ASSET_KNOX.label && m.get('action') == "delete")
						that.template = KnoxPolicyDeleteUpdateOperationDiff_tmpl;
				} else if(m.get('attributeName') == 'Policy Name'){
					if(m.get('action') != 'delete'){
						that.policyName = m.get('newValue');
					} else {
						that.policyName = m.get('previousValue');
					}
					if(m.get('newValue') == m.get('previousValue'))
						modelColl.push(m);
				}
			
				if(_.isUndefined(m.get('attributeName')))
					modelColl.push(m);
			});
			
			this.newGroupPermList 		= _.groupBy(this.newGroupPermList, 'groupName');
			this.previousGroupPermList 	= _.groupBy(this.previousGroupPermList, 'groupName');
			this.newUserPermList 			= _.groupBy(this.newUserPermList, 'userName');
			this.previousUserPermList 			= _.groupBy(this.previousUserPermList, 'userName');
			
			this.removeUnwantedElement();
			this.createEqualLengthArr();
			
			if(!_.isEmpty(this.newGroupPermList) || !_.isEmpty(this.previousGroupPermList))
				this.isGroupPerm = true;
			if(!_.isEmpty(this.newUserPermList) || !_.isEmpty(this.previousUserPermList))
				this.isUserPerm = true;
			
			that.collection.remove(modelColl);
		},
		removeUnwantedElement : function(){
			var that = this;
			_.each(this.newGroupPermList,function(val,key){
				console.log(val);
				that.newGroupPermList[key]	= _.uniq(val,false,function(m){return m.permType;});
			});
			_.each(this.previousGroupPermList,function(val,key){
				console.log(val);
				that.previousGroupPermList[key]	= _.uniq(val,false,function(m){return m.permType;});
			});
			_.each(this.newUserPermList,function(val,key){
				console.log(val);
				that.newUserPermList[key]	= _.uniq(val,false,function(m){return m.permType;});
			});
			_.each(this.previousUserPermList,function(val,key){
				console.log(val);
				that.previousUserPermList[key]	= _.uniq(val,false,function(m){return m.permType;});
			});
			
		},
		createEqualLengthArr : function(){
			if(this.objectSize(this.previousGroupPermList) > this.objectSize(this.newGroupPermList)){
				var addlength = this.objectSize(this.previousGroupPermList) - this.objectSize(this.newGroupPermList);
				for(var i=0; i < addlength; i++)
					this.newGroupPermList['temp'+i] = [];
			}else{
				var addlength = this.objectSize(this.newGroupPermList) - this.objectSize(this.previousGroupPermList);
				for(var i=0; i < addlength; i++)
					this.previousGroupPermList['temp'+i] = [];
			}
			if(this.objectSize(this.previousUserPermList) > this.objectSize(this.newUserPermList)){
				var addlength = this.objectSize(this.previousUserPermList) - this.objectSize(this.newUserPermList);
				for(var i=0; i < addlength; i++)
					this.newUserPermList['temp'+i] = [];
			}else{
				var addlength = this.objectSize(this.newUserPermList) - this.objectSize(this.previousUserPermList);
				for(var i=0; i < addlength; i++)
					this.previousUserPermList['temp'+i] = [];
			}
		},
		userDiffOperation : function(){
			var that = this, modelArr = [];
			this.groupList = [], this.newGroupList = [], this.previousGroupList =[],this.isGroup = false;
			
			this.collection.each(function(m){
				if(m.get('attributeName') == 'Group Name'){
					if(m.get('action') == 'create' || m.get('action') == 'update')
						that.newGroupList.push(m.get('parentObjectName'));
					if(m.get('action') == 'delete' || m.get('action') == 'update')
						that.previousGroupList.push(m.get('parentObjectName'));
					modelArr.push(m);
				} else if(m.get('attributeName') == 'User Role'){
					var newRole;
					if(!_.isUndefined(m.get('newValue'))){
						newRole =  m.get('newValue').replace(/[[\]]/g,'');
					}
					var prevRole = m.get('previousValue').replace(/[[\]]/g,'');
					if( newRole == "ROLE_USER")
						m.set('newValue',XAEnums.UserRoles.ROLE_USER.label)
					else if(newRole == "ROLE_SYS_ADMIN")
						m.set('newValue',XAEnums.UserRoles.ROLE_SYS_ADMIN.label)
					else if(newRole == "ROLE_KEY_ADMIN")
						m.set('newValue',XAEnums.UserRoles.ROLE_KEY_ADMIN.label)
					if(prevRole == "ROLE_USER")
						m.set('previousValue',XAEnums.UserRoles.ROLE_USER.label)
					else if(prevRole == "ROLE_SYS_ADMIN")
						m.set('previousValue',XAEnums.UserRoles.ROLE_SYS_ADMIN.label)
					else if(prevRole == "ROLE_KEY_ADMIN")
						m.set('previousValue',XAEnums.UserRoles.ROLE_KEY_ADMIN.label)
				} else {
					if(!m.has('attributeName'))
						modelArr.push(m);
				}
			});
			if(!_.isEmpty(this.newGroupList) || !_.isEmpty(this.previousGroupList)){
				this.isGroup = true;
			}
			this.collection.remove(modelArr);
		},
		groupDiffOperation : function(){
			var modelArr = [];
			this.collection.each(function(m){
				if(m.get('attributeName') == 'Group Name' && m.get('action') == 'create')
					modelArr.push(m);
			});
			this.collection.remove(modelArr);
		},	
		removeUnwantedFromObject : function(obj){
			_.each(obj, function(val, key){
					if(_.isEmpty(val))
						delete obj[key];
				});
			return obj;
		},
		objectSize : function(obj) {
		    var size = 0, key;
		    for (key in obj) {
		        if (obj.hasOwnProperty(key)) size++;
		    }
		    return size;
		},
		
		/** all post render plugin initialization */
		initializePlugins: function(){
		},
		/** on close */
		onClose: function(){
		}

	});

	return OperationDiffDetail;
});
