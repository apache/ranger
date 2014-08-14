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

	var XABaseModel	= require('models/XABaseModel');
	var XAGlobals	= require('utils/XAGlobals');

	var VXAssetBase = XABaseModel.extend(
	/** @lends VXAssetBase.prototype */
	{
		urlRoot: XAGlobals.baseURL + 'assets/assets',
		
		defaults: {},

		serverSchema : {
			"id" : {
				"dataType" : "Long"
			},
			"version" : {
				"dataType" : "int"
			},
			"createDate" : {
				"dataType" : "Date"
			},
			"updateDate" : {
				"dataType" : "Date"
			},
			"permList" : {
				"dataType" : "list",
				"listType" : "VNameValue"
			},
			"forUserId" : {
				"dataType" : "Long"
			},
			"status" : {
				"dataType" : "int"
			},
			"priGrpId" : {
				"dataType" : "Long"
			},
			"updatedBy" : {
				"dataType" : "String"
			},
			"isSystem" : {
				"dataType" : "boolean"
			},
			"name" : {
				"dataType" : "String"
			},
			"description" : {
				"dataType" : "String"
			},
			"activeStatus" : {
				"dataType" : "int"
			},
			"assetType" : {
				"dataType" : "int"
			},
			"config" : {
				"dataType" : "String"
			},
			"supportNative" : {
				"dataType" : "boolean"
			}
		},
		
		
		idAttribute: 'id',

		/**
		 * VXAssetBase initialize method
		 * @augments XABaseModel
		 * @constructs
		 */
		initialize: function() {
			this.modelName = 'VXAssetBase';
			//this.bind("error", XAUtils.defaultErrorHandler);
			this.bindErrorEvents();
		},
		
		testConfig : function(vAssest, options){
			var url = this.urlRoot  + '/testConfig';
			
			options = _.extend({
				data : JSON.stringify(vAssest),
				contentType : 'application/json',
				dataType : 'json'
			}, options);

			return this.constructor.nonCrudOperation.call(this, url, 'POST', options);
		},
		testKnoxCofig : function(vAssest, data, options){
			var url = 'service/assets/knox/resources?dataSourceName='+data.dataSourceName;
			
			options = _.extend({
//				data : JSON.stringify(data),
				contentType : 'application/json',
				dataType : 'json'
			}, options);

			return this.constructor.nonCrudOperation.call(this, url, 'GET', options);
		},

	}, {
		// static class members
	});

    return VXAssetBase;
	
});


