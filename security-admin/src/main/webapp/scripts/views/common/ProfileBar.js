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

	var App				= require('App');
    var Backbone		= require('backbone');
	var Communicator	= require('communicator');
	var SessionMgr 		= require('mgrs/SessionMgr');
	
	var ProfileBar_tmpl = require('hbs!tmpl/common/ProfileBar_tmpl'); 
	
	var ProfileBar = Backbone.Marionette.ItemView.extend(
	/** @lends ProfileBar */
	{
		_viewName : ProfileBar,
		
    	template: ProfileBar_tmpl,
    	templateHelpers : function(){
    		return {
    			userProfile : this.userProfile
    		};
    	},
        
    	/** ui selector cache */
    	ui: {
    		logout : 'a[data-id="logout"]'
    	},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			events['click ' + this.ui.logout]  = 'onLogout';
			return events;
		},
		onLogout : function(){
			var url = 'security-admin-web/logout.html';
			$.ajax({
				url : url,
				type : 'GET',
				headers : {
					"cache-control" : "no-cache"
				},
				success : function() {
					window.location.replace('login.jsp');
				},
				error : function(jqXHR, textStatus, err ) {
				}
				
			});
		},
    	/**
		* intialize a new ProfileBar ItemView 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a ProfileBar ItemView");

			_.extend(this, _.pick(options, ''));

			this.userProfile = SessionMgr.getUserProfile();
			this.bindEvents();
		},

		/** all events binding here */
		bindEvents : function(){
			//this.listenTo(this.userProfile, "change", this.render, this);
			this.listenTo(Communicator.vent,'ProfileBar:rerender', this.render, this);
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

	return ProfileBar;
});
