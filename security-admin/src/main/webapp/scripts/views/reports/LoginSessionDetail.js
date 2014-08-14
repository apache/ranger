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
	var App				= require('App');
	var XAEnums 		= require('utils/XAEnums');
	var XAEnums 		= require('utils/XAUtils');
	var XALinks			= require('modules/XALinks');
	var LoginSessionDetail_tmpl = require('hbs!tmpl/reports/LoginSessionDetail_tmpl');
	
	var LoginSessionDetail = Backbone.Marionette.ItemView.extend(
	/** @lends LoginSessionDetail */
	{
		_viewName : LoginSessionDetail,
		
    	template: LoginSessionDetail_tmpl,
        templateHelpers :function(){
        	
        	return {
        	};
        },
    	/** ui selector cache */
    	ui: {
    		loginTime : '[data-id="loginTime"]'
    	},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			return events;
		},

    	/**
		* intialize a new LoginSessionDetail ItemView 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a LoginSessionDetail ItemView");

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
			App.sessionId = this.model.get('id');
			var date = new Date(this.model.get('authTime')).toString();
			var timezone = date.replace(/^.*GMT.*\(/, "").replace(/\)$/, "");
			this.ui.loginTime.html(Globalize.format(new Date(this.model.get('authTime')),  "MM/dd/yyyy hh:mm:ss tt")+' '+timezone);
		},
		/** all post render plugin initialization */
		initializePlugins: function(){
		},
		/** on close */
		onClose: function(){
		}

	});

	return LoginSessionDetail;
});
