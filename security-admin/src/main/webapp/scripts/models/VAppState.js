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
/*
 * The singleton class for App State model to be used globally
 */

define(function(require) {
	'use strict';
	var XABaseModel	= require('models/XABaseModel');
	var XAGlobals	= require('utils/XAGlobals');

	var VAppState = XABaseModel.extend({
		defaults : {
			currentTab : XAGlobals.AppTabs.Dashboard.value
		},
		initialize : function() {
			this.modelName = 'VAppState';
		//	this.listenTo(this, 'change:currentAccount', this.accountChanged);
		}
		
	});

	// Make this a singleton!!
	return new VAppState();
});

