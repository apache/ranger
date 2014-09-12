/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
