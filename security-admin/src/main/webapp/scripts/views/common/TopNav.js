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

	var TopNav_tmpl = require('hbs!tmpl/common/TopNav_tmpl'); 
	require('jquery.cookie');
	var TopNav = Backbone.Marionette.ItemView.extend(
	/** @lends TopNav */
	{
		_viewName : TopNav,
		
    	template: TopNav_tmpl,
    	templateHelpers : function(){
    		
    	},
        
    	/** ui selector cache */
    	ui: {},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			events['click li']  = 'onNavMenuClick';
			return events;
		},

    	/**
		* intialize a new TopNav ItemView 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a TopNav ItemView");
			_.extend(this, _.pick(options, ''));
			this.bindEvents();
			this.appState = options.appState;
        	this.appState.on('change:currentTab', this.highlightNav,this);
		},

		/** all events binding here */
		bindEvents : function(){
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
		},

		/** on render callback */
		onRender: function() {
			var that = this;
			this.initializePlugins();
			$('.page-logo').on('click',function(){
				that.$('ul li').removeClass('active');
				that.$('ul li:first').addClass('active');
			});
			$.cookie('clientTimeOffset', new Date().getTimezoneOffset());
		},

		/** all post render plugin initialization */
		initializePlugins: function(){
		},
		onNavMenuClick : function(e){
			var ele = $(e.currentTarget);
			this.$('ul li').removeClass('active');
			ele.addClass('active');
		},
		highlightNav : function(e){
			this.$('ul li').removeClass('active');
        	this.$('#nav' + this.appState.get('currentTab')).parent().addClass('active');
        },
		/** on close */
		onClose: function(){
		}

	});

	return TopNav;
});
