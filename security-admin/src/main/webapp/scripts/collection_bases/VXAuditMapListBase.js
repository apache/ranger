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
	var VXAuditMap			= require('models/VXAuditMap');

	var VXAuditMapListBase = XABaseCollection.extend(
	/** @lends VXAuditMapListBase.prototype */
	{
		url: XAGlobals.baseURL + 'xusers/auditmaps',

		model : VXAuditMap,

		/**
		 * VXAuditMapListBase initialize method
		 * @augments XABaseCollection
		 * @constructs
		 */
		initialize : function() {
			this.modelName = 'VXAuditMap';
			this.modelAttrName = 'vXAuditMaps';
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

    return VXAuditMapListBase;
});


