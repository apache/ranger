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

	var Backbone		= require('backbone');
	var Communicator	= require('communicator');

	var Footer_tmpl = require('hbs!tmpl/common/Footer_tmpl'); 
	
	var Footer = Backbone.Marionette.ItemView.extend(
	/** @lends Footer */
	{
		_viewName : Footer,
		
    	template: Footer_tmpl,
        
    	/** ui selector cache */
    	ui: {},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			return events;
		},

    	/**
		* intialize a new Footer ItemView 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a Footer ItemView");

			_.extend(this, _.pick(options, ''));

			this.bindEvents();
		},

		/** all events binding here */
		bindEvents : function(){
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
		},

		/** on render callback */
		onRender: function() {
			this.initializePlugins();
		},

		/** all post render plugin initialization */
		initializePlugins: function(){
		},

		/** on close */
		onClose: function(){
			
		}

	});

	return Footer;
});
