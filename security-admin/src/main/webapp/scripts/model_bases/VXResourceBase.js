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


	var VXResourceBase = XABaseModel.extend(
	/** @lends VXResourceBase.prototype */
	{
		urlRoot: XAGlobals.baseURL + 'assets/resources',
		
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
			"resourceType" : {
				"dataType" : "int"
			},
			"assetId" : {
				"dataType" : "Long"
			},
			"parentId" : {
				"dataType" : "Long"
			},
			"parentPath" : {
				"dataType" : "String"
			},
			"isEncrypt" : {
				"dataType" : "int"
			},
			"permMapList" : {
				"dataType" : "list",
				"listType" : "VXPermMap"
			},
			"auditList" : {
				"dataType" : "list",
				"listType" : "VXAuditMap"
			}
		},
		
		idAttribute: 'id',

		/*modelRel: {
			permMapList: VXPermMapList,
		},*/
		//localStorage: new Backbone.LocalStorage("VXResourceModel"),

		/**
		 * VXResourceBase initialize method
		 * @augments XABaseModel
		 * @constructs
		 */
		initialize: function() {
			this.modelName = 'VXResourceBase';
			//this.permMapList = this.constructor.nestCollection(this, 'permMapList', new VXPermMapList(this.get('permMapList')));
			//this.auditList = this.constructor.nestCollection(this, 'auditList', new VXAuditMapList(this.get('auditList')));
			
		}

	}, {
		// static class members
	});

    return VXResourceBase;
	
});


