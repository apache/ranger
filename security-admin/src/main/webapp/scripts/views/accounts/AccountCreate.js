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

    var App		= require('App');
	var Backbone		= require('backbone');
	var Communicator	= require('communicator');
	var XAUtil			= require('utils/XAUtils');
	var XALinks 		= require('modules/XALinks');
	
	var AccountCreate_tmpl = require('hbs!tmpl/accounts/AccountCreate_tmpl'); 
	var AccountForm = require('views/accounts/AccountForm');

	var AccountCreate = Backbone.Marionette.Layout.extend(
	/** @lends AccountCreate */
	{
		_viewName : 'AccountCreate',
		
    	template: AccountCreate_tmpl,
    	breadCrumbs :function(){
    		if(this.model.isNew())
    			return [XALinks.get('Accounts'), XALinks.get('AccountCreate')];
    		else
    			return [XALinks.get('Accounts'), XALinks.get('AccountEdit')];
    	} ,
        
		/** Layout sub regions */
    	regions: {
			'rForm' :'div[data-id="r_form"]'
		},

    	/** ui selector cache */
    	ui: {
			'btnSave'	: '[data-id="save"]',
			'btnCancel' : '[data-id="cancel"]'
		},

		/** ui events hash */
		events: function() {
			var events = {};
			events['click ' + this.ui.btnSave]		= 'onSave';
			events['click ' + this.ui.btnCancel]	= 'onCancel';
			return events;
		},

    	/**
		* intialize a new AccountCreate ItemView 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a AccountCreate ItemView");

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
			this.form = new AccountForm({
				model : this.model,
				template : require('hbs!tmpl/accounts/AccountForm_tmpl')
			});
			this.rForm.show(this.form);
			if(!this.model.isNew()){
				this.form.setUpSwitches();
			}
			this.initializePlugins();
		},

		/** all post render plugin initialization */
		initializePlugins: function(){
		},

		onSave: function(){
			var errors = this.form.commit({validate : false});
			if(! _.isEmpty(errors)){
				return;
			}
			this.model.save();
			/*this.model.save({},{
				wait: true,
				success: function () {
					var msg = 'Account created successfully';
					XAUtil.notifySuccess('Success', msg);
					App.appRouter.navigate("#!/accounts",{trigger: true});
					console.log("success");
				},
				error: function (model, response, options) {
					XAUtil.notifyError('Error', 'Error creating Account!');
					console.log("error");
				}
			});*/
		},
		onCancel: function(){
			App.appRouter.navigate("#!/accounts",{trigger: true});
		},
		/** on close */
		onClose: function(){
			App.appRouter.navigate("#!/accounts",{trigger: true});
		}

	});

	return AccountCreate;
});
