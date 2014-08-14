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
	var Handlebars		= require('handlebars');
	var Communicator	= require('communicator');

	
	var Spinner = Backbone.Marionette.Layout.extend(
	/** @lends Spinner */
	{
		_viewName : 'Spinner',
		
		template  : Handlebars.compile("<span></span>"),

		tagName	  : 'span',
        
    	/** ui selector cache */
    	ui: {},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			return events;
		},

    	/**
		* intialize a new Spinner Layout 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a Spinner ItemView");

			_.extend(this, _.pick(options, 'collection'));

			this.bindEvents();
		},

		/** all events binding here */
		bindEvents : function(){
            this.listenTo(this.collection, 'request', this.displaySpinner);
            this.listenTo(this.collection, 'sync error', this.removeSpinner);
		},

		/** on render callback */
		onRender: function() {
			this.initializePlugins();
		},

		/** all post render plugin initialization */
		initializePlugins: function(){
		},

		displaySpinner: function () {
            this.$el.empty().append('<i class="icon-spinner icon-spin icon-3x white spin-position"></i>');
        },

        removeSpinner: function () {
            this.$el.empty();
        },
		
		/** on close */
		onClose: function(){
		}

	});

	return Spinner;
});
