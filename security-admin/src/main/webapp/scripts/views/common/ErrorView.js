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
	
	var ErrorView_tmpl = require('hbs!tmpl/common/ErrorView_tmpl'); 
	
	var ErrorView = Backbone.Marionette.ItemView.extend(
	/** @lends ErrorView */
	{
		_viewName : ErrorView,
		
    	template: ErrorView_tmpl,
        templateHelpers :function(){
        	return {
        		restrictedAccess :this.restrictedAccess || false,
        		pageNotFound :this.pageNotFound || false
        	};
        },
    	/** ui selector cache */
    	ui: {
    		'goBackBtn' : 'a[data-id="goBack"]',
    		'home' 		: 'a[data-id="home"]'
    	},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			events['click ' + this.ui.goBackBtn]  = 'goBackClick';
			return events;
		},

    	/**
		* intialize a new ErrorView ItemView 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a ErrorView ItemView");

			_.extend(this, _.pick(options, 'restrictedAccess','pageNotFound'));

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
			$('#r_breadcrumbs').hide();
		},
		goBackClick : function(){
			
		},

		/** all post render plugin initialization */
		initializePlugins: function(){
		},
		/** on close */
		onClose: function(){
			$('#r_breadcrumbs').show();
		}

	});

	return ErrorView;
});
