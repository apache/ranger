/*
 * Copyright (c) 2014 XASecure
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure
 */

define(function(require){
	var XABaseModel = require('models/XABaseModel');
	var XAEnums 	= require('utils/XAEnums');

	return XABaseModel.extend({
		initialize : function() {
			this.modelName = 'VPasswordChange';
			this.myClassType = XAEnums.ClassTypes.CLASS_TYPE_PASSWORD_CHANGE.value;
		}
	});

});

