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

	var VXAuditRecordBase = XABaseModel.extend(
	/** @lends VXAuditRecordBase.prototype */
	{
		urlRoot: XAGlobals.baseURL + 'assets/audit/report',
		
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
			"date" : {
				"dataType" : "Date"
			},
			"resource" : {
				"dataType" : "String"
			},
			"action" : {
				"dataType" : "String"
			},
			"result" : {
				"dataType" : "String"
			},
			"user" : {
				"dataType" : "String"
			},
			"enforcer" : {
				"dataType" : "String"
			}
		},
		
		
		idAttribute: 'id',

		/**
		 * VXAuditRecordBase initialize method
		 * @augments XABaseModel
		 * @constructs
		 */
		initialize: function() {
			this.modelName = 'VXAuditRecordBase';
		}

	}, {
		// static class members
	});

    return VXAuditRecordBase;
	
});


