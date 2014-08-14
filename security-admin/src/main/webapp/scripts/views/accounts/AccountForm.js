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
	var localization = require('utils/XALangSupport');
	var XAEnums 	= require('utils/XAEnums');
	
	require('Backbone.BootstrapModal');
	require('backbone-forms.list');
	require('backbone-forms.templates');
	require('backbone-forms');

	var AccountForm = Backbone.Form.extend(
	/** @lends AccountForm */
	{
		_viewName : 'AccountForm',
    	/**
		* intialize a new AccountForm Form View 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a AccountForm Form View");
    		Backbone.Form.prototype.initialize.call(this, options);

			_.extend(this, _.pick(options, ''));

			this.bindEvents();
		},
		
		/*schema : function(){
			return _.pick(_.result(this.model.schema),'customerName', 'accountStatus', 'accountCode');
		},
*/
		/** all events binding here */
		bindEvents : function(){
			/*this.on('field:change', function(form, fieldEditor){
    			this.evFieldChange(form, fieldEditor);
    		});*/
		},

		/** on render callback */
		onRender: function() {
			this.initializePlugins();
			/*if(!this.model.isNew()){
				this.setUpSwitches();
			}*/
		},
		setUpSwitches :function(){
			var that = this;
			var accountStatus = false;
			_.each(_.toArray(XAEnums.BooleanValue),function(m){
				if(parseInt(that.model.get('accountStatus')) == m.value)
					accountStatus =  (m.label == XAEnums.BooleanValue.BOOL_TRUE.label) ? true : false;
			});
			this.fields.accountStatus.editor.setValue(accountStatus);
		},

		/** all post render plugin initialization */
		initializePlugins: function(){
		}

	});

	return AccountForm;
});
