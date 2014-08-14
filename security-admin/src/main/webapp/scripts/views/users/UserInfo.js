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
	var XAEnums 		= require('utils/XAEnums');
	var UserInfo_tmpl = require('hbs!tmpl/users/UserInfo_tmpl'); 
	
	var UserInfo = Backbone.Marionette.ItemView.extend(
	/** @lends UserInfo */
	{
		_viewName : 'UserInfo',
		
    	template: UserInfo_tmpl,
    	templateHelpers : function(){
    		return {
    			'groupList' : this.groupList,
    			'userList'	: this.userList,
    			'userModel'	: this.model.modelName == XAEnums.ClassTypes.CLASS_TYPE_XA_USER.modelName ? true : false,
    			'groupModel': this.model.modelName == XAEnums.ClassTypes.CLASS_TYPE_XA_GROUP.modelName ? true : false
    		};
    	},
        
    	/** ui selector cache */
    	ui: {
    		'showMoreBtn' : '[data-id="showMore"]',
    		'showLessBtn' : '[data-id="showLess"]'
    	},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			events['click ' + this.ui.showMoreBtn]  = 'onShowMoreClick';
			events['click ' + this.ui.showLessBtn]  = 'onShowLessClick';
			return events;
		},

    	/**
		* intialize a new UserInfo ItemView 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a UserInfo ItemView");

			_.extend(this, _.pick(options, 'groupList','userList'));
			
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
			if(this.ui.showMoreBtn.parent('td').find('span').length > 3){
				this.ui.showMoreBtn.show();	
				this.ui.showMoreBtn.parent('td').find('span').slice(3).hide();
			}
		},
		onShowMoreClick : function(){
			this.ui.showMoreBtn.parent('td').find('span').show();
			this.ui.showMoreBtn.hide();
			this.ui.showLessBtn.show();
		},
		onShowLessClick : function(){
			this.ui.showMoreBtn.parent('td').find('span').slice(3).hide();
			this.ui.showLessBtn.hide();
			this.ui.showMoreBtn.show();
		},

		/** all post render plugin initialization */
		initializePlugins: function(){
		},

		/** on close */
		onClose: function(){
		}

	});

	return UserInfo;
});
