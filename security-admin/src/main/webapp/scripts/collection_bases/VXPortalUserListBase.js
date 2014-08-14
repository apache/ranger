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
	var VXPortalUser		= require('models/VXPortalUser');

	var VXPortalUserListBase = XABaseCollection.extend(
	/** @lends VXPortalUserListBase.prototype */
	{
		url: XAGlobals.baseURL + '',

		model : VXPortalUser,

		/**
		 * VXPortalUserListBase initialize method
		 * @augments XABaseCollection
		 * @constructs
		 */
		initialize : function() {
			this.modelName = 'VXPortalUser';
			this.modelAttrName = 'vXPortalUsers';
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

    return VXPortalUserListBase;
});


