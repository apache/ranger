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

	var VXAuthSessionBase	= require('model_bases/VXAuthSessionBase');

	var VXAuthSession = VXAuthSessionBase.extend(
	/** @lends VXAuthSession.prototype */
	{
		/**
		 * VXAuthSessionBase initialize method
		 * @augments XABaseModel
		 * @constructs
		 */
		initialize: function() {
			this.modelName = 'VXAuthSession';
			this.bindErrorEvents();
		},
		
		/** This models toString() */
		toString : function(){
			return this.get('name');
		}

	}, {
		// static class members
	});

    return VXAuthSession;
	
});


