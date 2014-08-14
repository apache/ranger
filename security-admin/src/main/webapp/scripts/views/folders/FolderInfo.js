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

define(function(require){
    'use strict';

	var Backbone		= require('backbone');
	var Communicator	= require('communicator');
	
	var XAUtil			= require('utils/XAUtils');
	var XAEnums			= require('utils/XAEnums');
	var VXPermMapList 	= require('collections/VXPermMapList');
	var FolderInfo_tmpl = require('hbs!tmpl/folders/FolderInfo_tmpl');
	var KnoxInfo_tmpl = require('hbs!tmpl/knox/KnoxInfo_tmpl');

	var FolderInfo = Backbone.Marionette.ItemView.extend(
	/** @lends FolderInfo */
	{
		_viewName : FolderInfo,
		
    	template: FolderInfo_tmpl,
        templateHelpers :function(){
        	
        	var hiveModel = this.model.get('assetType') == XAEnums.AssetType.ASSET_HIVE.value ? true : false;
        	var hbaseModel = this.model.get('assetType') == XAEnums.AssetType.ASSET_HBASE.value ? true : false;
        	var hiveOrHbaseModel = hiveModel || hbaseModel;
        	return {
        		groupPermListArr: this.getGroupPermListArr(),
        		userPermListArr : this.getUserPermListArr(),
   				hdfsModel 		: this.model.get('assetType') == XAEnums.AssetType.ASSET_HDFS.value ? true : false,
        		hiveOrHbaseModel: hiveOrHbaseModel,
        		
        	};
        },
    	/** ui selector cache */
    	ui: {},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			return events;
		},

    	/**
		* intialize a new FolderInfo ItemView 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a FolderInfo ItemView");

			_.extend(this, _.pick(options, ''));
			//this.model.set('permMapList',new VXPermMapList(this.model.attributes.permMapList))
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
		},

		/** all post render plugin initialization */
		initializePlugins: function(){
		},
		getTemplateForView : function(){
			if(this.model.get('assetType') == XAEnums.AssetType.ASSET_KNOX.value){
				this.template = KnoxInfo_tmpl;
			}
		},
		getGroupPermListArr : function(){
			var groupList = [];
			var perm=[],permListArr =[];
			if(!_.isUndefined(this.model.attributes.permMapList)){
				if(_.isArray(this.model.attributes.permMapList)){
					groupList = new VXPermMapList(this.model.attributes.permMapList);
					groupList =  groupList.groupBy('groupId');
				}else{
					groupList =  this.model.attributes.permMapList.groupBy('groupId');
				}
				
				_.each(groupList,function(val,prop){
					var groups = groupList[prop];
					var permTypeArr =[],name ='',ipAddressArr =[];
					groups = _.sortBy(groups, function (m) {return m.get('permType');});
					_.each(groups,function(gprop){
						var permVal = _.pick(gprop.attributes,'permType').permType;
						var label = XAUtil.enumValueToLabel(XAEnums.XAPermType,permVal);
						permTypeArr.push(label);
						name = _.pick(gprop.attributes,'groupName').groupName;
						if(!_.isUndefined(gprop.get('ipAddress')))
							ipAddressArr.push(gprop.get('ipAddress'))
								
					});
					if(name != undefined){
						var ipAddress = _.uniq(ipAddressArr).toString();
						permListArr.push({
							name :name,
							permType :permTypeArr.toString(),
							ipAddress :_.isEmpty(ipAddress) ? "--" : ipAddress
						});
					}
					
				});
			}
        	return permListArr;
	     },
	     getUserPermListArr : function(){
    	 	var userList = [];
    	 	var perm=[],permListArr =[];
    	 	if(!_.isUndefined(this.model.attributes.permMapList)){
	    		 if(_.isArray(this.model.attributes.permMapList)){
	    			 userList = new VXPermMapList(this.model.attributes.permMapList);
	    			 userList =  userList.groupBy('userId');
	    		 }else{
	    			 userList =  this.model.attributes.permMapList.groupBy('userId');
	    		 }
	    		 //var userList =  this.model.attributes.permMapList.groupBy('userId');
	    		 _.each(userList,function(val,prop){
	    			 var users = userList[prop];
	    			 users = _.sortBy(users, function (m) {return m.get('permType');});
	    			 var permTypeArr =[],name ='', ipAddressArr =[];
	    			 _.each(users,function(gprop){
	    				 var permVal = _.pick(gprop.attributes,'permType').permType;
	    				 var label = XAUtil.enumValueToLabel(XAEnums.XAPermType,permVal);
	    				 permTypeArr.push(label);
	    				 name = _.pick(gprop.attributes,'userName').userName;
	    				 if(!_.isUndefined(gprop.get('ipAddress')))
							ipAddressArr.push(gprop.get('ipAddress'))
	    			 });
	    			 if(name != undefined){
	    				 var ipAddress = _.uniq(ipAddressArr).toString();	    				 
	    				 permListArr.push({ name :name,
	    					 permType :permTypeArr.toString(),
	    					 ipAddress :_.isEmpty(ipAddress) ? "--" : ipAddress
	    				 });
	    			 }
	    			 
	    		 });
	    	 }	
	        return permListArr;
		     },
		/** on close */
		onClose: function(){
		}

	});

	return FolderInfo;
});
