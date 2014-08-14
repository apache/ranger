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
	var VXAccessAudit		= require('models/VXAccessAudit');

	var VXAccessAuditListBase = XABaseCollection.extend(
	/** @lends VXAccessAuditListBase.prototype */
	{
		url: XAGlobals.baseURL + 'assets/accessAudit',

		model : VXAccessAudit,

		/**
		 * VXAccessAuditListBase initialize method
		 * @augments XABaseCollection
		 * @constructs
		 */
		initialize : function() {
			this.modelName = 'VXAccessAudit';
			this.modelAttrName = 'vXAccessAudits';
			this.bindErrorEvents();
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

    return VXAccessAuditListBase;
});


