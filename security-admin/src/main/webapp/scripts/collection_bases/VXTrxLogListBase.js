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
	var VXTrxLog			= require('models/VXTrxLog');

	var VXTrxLogListBase = XABaseCollection.extend(
	/** @lends VXTrxLogListBase.prototype */
	{
		url: XAGlobals.baseURL + 'assets/report',

		model : VXTrxLog,

		/**
		 * VXTrxLogListBase initialize method
		 * @augments XABaseCollection
		 * @constructs
		 */
		initialize : function() {
			this.modelName = 'VXTrxLog';
			this.modelAttrName = 'vXTrxLogs';
			this.bindErrorEvents();
		},
		getFullTrxLogListForTrxId : function(trxId, options){
			var url = this.url  + '/' + trxId ;
			
			options = _.extend({
				//	data : JSON.stringify(postData),
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

    return VXTrxLogListBase;
});


