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

	var AccountdetaillayoutTmpl = require('hbs!tmpl/accounts/AccountDetailLayout_tmpl');
	

	var AccountDetailLayout = Backbone.Marionette.Layout.extend(
	/** @lends AccountDetailLayout */
	{
		_viewName : 'AccountDetailLayout',
		
    	template: AccountdetaillayoutTmpl,
        
		/** Layout sub regions */
    	regions: {
    		rAccountDetail : '#r_accDetail'
    	},

    	/** ui selector cache */
    	ui: {},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			return events;
		},

    	/**
		* intialize a new AccountDetailLayout Layout 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a AccountDetailLayout Layout");

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
			this.renderDetailView();
		},
		renderDetailView : function(){
			var that = this;	
			
        },
		/** all post render plugin initialization */
		initializePlugins: function(){
		},

		/** on close */
		onClose: function(){
		}

	});

	return AccountDetailLayout; 
});
