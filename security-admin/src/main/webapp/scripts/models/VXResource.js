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

	var VXResourceBase	= require('model_bases/VXResourceBase');

	var VXResource = VXResourceBase.extend(
	/** @lends VXResource.prototype */
	{
		
		/**
		 * VXResource initialize method
		 * @augments FSBaseModel
		 * @constructs
		 */
		initialize: function() {
			this.modelName = 'VXResource';
			this.bindErrorEvents();
		},
		
		/** This models toString() */
		toString : function(){
			return this.get('name');
		}

	}, {
		// static class members
	});

    return VXResource;
	
});


