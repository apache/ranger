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

	var VXPortalUserBase = XABaseModel.extend(
	/** @lends VXPortalUserBase.prototype */
	{
		urlRoot: XAGlobals.baseURL + 'users',
		
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
			"loginId" : {
				"dataType" : "String"
			},
			"password" : {
				"dataType" : "String"
			},
			"profileImageGId" : {
				"dataType" : "Long"
			},
			"status" : {
				"dataType" : "int"
			},
			"emailAddress" : {
				"dataType" : "String"
			},
			"isTestUser" : {
				"dataType" : "boolean"
			},
			"isRegistered" : {
				"dataType" : "boolean"
			},
			"isInternal" : {
				"dataType" : "boolean"
			},
			"gender" : {
				"dataType" : "int"
			},
			"firstName" : {
				"dataType" : "String"
			},
			"lastName" : {
				"dataType" : "String"
			},
			"publicScreenName" : {
				"dataType" : "String"
			},
			"userSource" : {
				"dataType" : "int"
			},
			"timeZone" : {
				"dataType" : "String"
			},
			"userRoleList" : {
				"dataType" : "list",
				"listType" : "String"
			}
		},
		
		
		idAttribute: 'id',

		/**
		 * VXPortalUserBase initialize method
		 * @augments XABaseModel
		 * @constructs
		 */
		initialize: function() {
			this.modelName = 'VXPortalUserBase';
			this.bindErrorEvents();
		},

		/*************************
		 * Non - CRUD operations
		 *************************/

		getUserProfile: function(options) {
			return this.constructor.nonCrudOperation.call(this, this.urlRoot + '/profile', 'GET', options);
		},

		setUserRoles : function(userId, postData , options){
			var url = this.urlRoot  + '/' + userId + '/roles';
			
			options = _.extend({
				data : JSON.stringify(postData),
				contentType : 'application/json',
				dataType : 'json'
			}, options);

			return this.constructor.nonCrudOperation.call(this, url, 'PUT', options);
		},
		changePassword : function(userId, postData , options){
			var url = this.urlRoot  + '/' + userId + '/passwordchange';
			
			options = _.extend({
				data : JSON.stringify(postData),
				contentType : 'application/json',
				dataType : 'json'
			}, options);

			return this.constructor.nonCrudOperation.call(this, url, 'POST', options);
		},
		saveUserProfile : function(vXPortalUser, options){
			var url = this.urlRoot ;
			
			options = _.extend({
				data : JSON.stringify(vXPortalUser),
				contentType : 'application/json',
				dataType : 'json'
			}, options);

			return this.constructor.nonCrudOperation.call(this, url, 'PUT', options);
		}
		
	}, {
		// static class members
	});

    return VXPortalUserBase;
	
});


