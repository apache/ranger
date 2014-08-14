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


	var VXAuthSessionBase = XABaseModel.extend(
	/** @lends VXAuthSessionBase.prototype */
	{
		urlRoot: XAGlobals.baseURL + 'xusers/authSessions',
		
		defaults: {},
		
		idAttribute: 'id',

		/**
		 * VXAuthSessionBase initialize method
		 * @augments XABaseModel
		 * @constructs
		 */
		initialize: function() {
			this.modelName = 'vXAuthSessionBase';
		}

	}, {
		// static class members
	});

    return VXAuthSessionBase;
	
});


