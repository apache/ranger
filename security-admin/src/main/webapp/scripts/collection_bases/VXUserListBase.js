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
	var VXUser				= require('models/VXUser');

	var VXUserListBase = XABaseCollection.extend(
	/** @lends VXUserListBase.prototype */
	{
		url: XAGlobals.baseURL + 'xusers/users',

		model : VXUser,

		/**
		 * VXUserListBase initialize method
		 * @augments XABaseCollection
		 * @constructs
		 */
		initialize : function() {
			this.modelName = 'VXUser';
			this.modelAttrName = 'vXUsers';
			this.bindErrorEvents();
		},
		

		/*************************
		 * Non - CRUD operations
		 *************************/

		getUsersOfGroup : function(groupId, options){
			var url = XAGlobals.baseURL  + 'xusers/'  + groupId + '/users';
			
			options = _.extend({
				//data : JSON.stringify(postData),
				contentType : 'application/json',
				dataType : 'json'
			}, options);

			return this.constructor.nonCrudOperation.call(this, url, 'GET', options);
		}
	},{
	/**
	* Table Cols to be passed to Backgrid
	* UI has to use this as base and extend this.
	*
	*/

		tableCols : {}
	});

    return VXUserListBase;
});


