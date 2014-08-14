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
	
	var XAGlobals = {};
	
	XAGlobals.settings = {};
	XAGlobals.settings.PAGE_SIZE = 25;
	XAGlobals.settings.AUDIT_REPORT_POLLING = 100000;
	XAGlobals.settings.uploadDefaultOpts = {
		disableImageResize: false,
		maxFileSize: 5000000,
		autoUpload : false
		//maxNumberOfFiles : 2
	};
	XAGlobals.settings.MAX_VALUE = 2147483647;

	XAGlobals.keys = {};
	XAGlobals.keys.ENTER_KEY = 13;
	XAGlobals.keys.ESC_KEY = 27;

	//XAGlobals.baseURL = '../';
	XAGlobals.baseURL = 'service/';

	XAGlobals.version = 0;//0 : production version 1: any other
	XAGlobals.AppTabs = {
			Dashboard 			: { value:1, valStr: 'Dashboard'},
			PolicyManager		: { value:2, valStr: 'Policy'},
			Users 				: { value:3, valStr: 'Users'},
//			Reports 			: { value:4, valStr: 'Reports'},
			Config 				: { value:5, valStr: 'Config'},
			Assets				: { value:6, valStr: 'Assets'},
			Analytics			: { value:7, valStr: 'Analytics'},
			Audit				: { value:8, valStr: 'Analytics'},
			None				: { value:9, valStr: 'None'}
		};

	XAGlobals.BooleanValue = {
		BOOL_TRUE:{value:"true", label:'True'},
		BOOL_FALSE:{value:"false", label:'False'}
	};
	XAGlobals.hardcoded = {};
	XAGlobals.hardcoded.HDFSAssetId = 1;
	XAGlobals.hardcoded.HBaseAssetId = 2;
	XAGlobals.hardcoded.HiveAssetId = 3;
	
	return XAGlobals;
});
