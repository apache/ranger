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

	var XABaseCollection	= require('collections/XABaseCollection');
	var XAGlobals			= require('utils/XAGlobals');
	var VXGroup				= require('models/VXGroup');

	var VXGroupListBase = XABaseCollection.extend(
	/** @lends VXGroupListBase.prototype */
	{
		url: XAGlobals.baseURL + 'xusers/groups',

		model : VXGroup,

		/**
		 * VXGroupListBase initialize method
		 * @augments XABaseCollection
		 * @constructs
		 */
		initialize : function() {
			this.modelName = 'VXGroup';
			this.modelAttrName = 'vXGroups';
			this.bindErrorEvents();
		},
		/*************************
		 * Non - CRUD operations
		 *************************/
		
		getGroupsForUser : function(userId, options){
			var url = XAGlobals.baseURL + 'xusers/' + userId + '/groups';
			
			options = _.extend({
				//data : JSON.stringify(postData),
				contentType : 'application/json',
				dataType : 'json'
			}, options);

			return this.constructor.nonCrudOperation.call(this, url, 'GET', options);
		}
	},{
		// static class members
		/**
		* Table Cols to be passed to Backgrid
		* UI has to use this as base and extend this.
		*
		*/

		tableCols : {}

	});

    return VXGroupListBase;
});


