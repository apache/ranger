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

 
/* 
 * Policy create view
 */

define(function(require){
    'use strict';

	var Backbone		= require('backbone');
	var App				= require('App');
	var XAEnums			= require('utils/XAEnums');
	var XAUtil			= require('utils/XAUtils');
	var XALinks 		= require('modules/XALinks');
	var localization	= require('utils/XALangSupport');
	
	var RangerPolicycreateTmpl = require('hbs!tmpl/policies/RangerPolicyCreate_tmpl');
	var RangerPolicyForm = require('views/policies/RangerPolicyForm');
	var RangerServiceDef	= require('models/RangerServiceDef');

	var RangerPolicyCreate = Backbone.Marionette.Layout.extend(
	/** @lends RangerPolicyCreate */
	{
		_viewName : 'RangerPolicyCreate',
		
    	template : RangerPolicycreateTmpl,
    	templateHelpers : function(){
    		return {
    			editPolicy : this.editPolicy
    		};
    	},
    	breadCrumbs :function(){
	    	
    		if(this.model.isNew())
    			return [XALinks.get('ServiceManager'),XALinks.get('ManagePolicies',{model : this.rangerService}),XALinks.get('PolicyCreate')];
    		else
    			return [XALinks.get('ServiceManager'),XALinks.get('ManagePolicies',{model : this.rangerService}),XALinks.get('PolicyEdit')];
    	} ,        

		/** Layout sub regions */
    	regions: {
			'rForm' :'div[data-id="r_form"]'
		},

    	/** ui selector cache */
    	ui: {
			'btnSave'	: '[data-id="save"]',
			'btnCancel' : '[data-id="cancel"]',
			'btnDelete' : '[data-id="delete"]',
			'policyDisabledAlert' : '[data-id="policyDisabledAlert"]' 
		},

		/** ui events hash */
		events: function() {
			var events = {};
			events['click ' + this.ui.btnSave]		= 'onSave';
			events['click ' + this.ui.btnCancel]	= 'onCancel';
			events['click ' + this.ui.btnDelete]	= 'onDelete';
			
			return events;
		},

    	/**
		* intialize a new RangerPolicyCreate Layout 
		* @constructs
		*/
		initialize: function(options) {
			var that = this;
			console.log("initialized a RangerPolicyCreate Layout");

			_.extend(this, _.pick(options, 'rangerService'));
			this.initializeServiceDef();
			that.form = new RangerPolicyForm({
				template : require('hbs!tmpl/policies/RangerPolicyForm_tmpl'),
				model : this.model,
				rangerServiceDefModel : this.rangerServiceDefModel,
				rangerService : this.rangerService
			});

			this.editPolicy = this.model.has('id') ? true : false;
			this.bindEvents();
			this.params = {};
		},
		initializeServiceDef : function(){
			
			this.rangerServiceDefModel	= new RangerServiceDef();
			this.rangerServiceDefModel.url = XAUtil.getRangerServiceDef(this.rangerService.get('type'));
			this.rangerServiceDefModel.fetch({
				cache : false,
				async : false
			})
		},

		/** all events binding here */
		bindEvents : function(){
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
		},

		/** on render callback */
		onRender: function() {
			XAUtil.showAlerForDisabledPolicy(this);
			this.rForm.show(this.form);
			this.rForm.$el.dirtyFields();
			XAUtil.preventNavigation(localization.tt('dialogMsg.preventNavPolicyForm'),this.rForm.$el);
		},
		popupCallBack : function(msg,validateObj){
			var that = this;
			XAUtil.alertPopup({
				msg :msg,
				callback : function(){
				//	if(validateObj.auditLoggin)
				//		that.savePolicy();
				}
			});
		},
		onSave: function(){
			var that = this, valid = false;
			var errors = this.form.commit({validate : false});
			if(! _.isEmpty(errors)){
				return;
			}
			var validateObj = this.form.formValidation();
			valid = (validateObj.groupSet && validateObj.permSet) || (validateObj.userSet && validateObj.userPerm);
			if(!valid){
				if(validateObj.groupSet && (!validateObj.permSet)){
					this.popupCallBack(localization.tt('msg.addGroupPermission'),validateObj);
				}else if((!validateObj.groupSet) && (validateObj.permSet)) {
					this.popupCallBack(localization.tt('msg.addGroup'),validateObj);
						
				}else if(validateObj.userSet && (!validateObj.userPerm)){
					this.popupCallBack(localization.tt('msg.addUserPermission'),validateObj);
				}else if((!validateObj.userSet) && (validateObj.userPerm)) {
					this.popupCallBack(localization.tt('msg.addUser'),validateObj);
						
				}else if((!validateObj.auditLoggin) && (!validateObj.groupPermSet)){
					XAUtil.alertPopup({
						msg :localization.tt('msg.yourAuditLogginIsOff'),
						callback : function(){
							/*if(!that.model.isNew()){
								that.model.destroy({success: function(model, response) {
									XAUtil.notifySuccess('Success', localization.tt('msg.policyDeleteMsg'));
									App.appRouter.navigate("#!/hdfs/"+that.assetModel.id+"/policies",{trigger: true});
								}});
							}else{
								XAUtil.notifyError('Error', localization.tt('msg.policyNotAddedMsg'));
								App.appRouter.navigate("#!/hdfs/"+that.assetModel.id+"/policies",{trigger: true});
							}*/
						}
					});
				}else{
					this.savePolicy();
				}
			}else{
				if(validateObj.groupSet && (!validateObj.permSet)){
					this.popupCallBack(localization.tt('msg.addGroupPermission'),validateObj);
				}else if((!validateObj.groupSet) && (validateObj.permSet)) {
					this.popupCallBack(localization.tt('msg.addGroup'),validateObj);
						
				}else if(validateObj.userSet && (!validateObj.userPerm)){
					this.popupCallBack(localization.tt('msg.addUserPermission'),validateObj);
				}else if((!validateObj.userSet) && (validateObj.userPerm)) {
					this.popupCallBack(localization.tt('msg.addUser'),validateObj);
						
				}else{
					this.savePolicy();
				}
			}
		},
		savePolicy : function(){
			var that = this;
			this.form.beforeSave();
			this.saveMethod();
		},
		saveMethod : function(){
			var that = this;
			XAUtil.blockUI();
			this.model.save({},{
				wait: true,
				success: function () {
					XAUtil.blockUI('unblock');
					var msg = that.editPolicy ? 'Policy updated successfully' :'Policy created successfully';
					XAUtil.notifySuccess('Success', msg);
					XAUtil.allowNavigation();
					if(that.editPolicy){
						App.appRouter.navigate("#!/service/"+that.rangerService.id+"/policies",{trigger: true});
						return;
					}
					App.appRouter.navigate("#!/service/"+that.rangerService.id+"/policies",{trigger: true});
					console.log("success");
				},
				error: function (model, response, options) {
					    XAUtil.blockUI('unblock');
						XAUtil.notifyError('Error', 'Error creating Policy!');
					    console.log("error");
				}
			});
		},
		onCancel : function(){
			XAUtil.allowNavigation();
			App.appRouter.navigate("#!/service/"+this.rangerService.id+"/policies",{trigger: true});

		},
		onDelete :function(){
			var that = this;
			XAUtil.confirmPopup({
				//msg :localize.tt('msg.confirmDelete'),
				msg :'Are you sure want to delete ?',
				callback : function(){
					XAUtil.blockUI();
					that.model.destroy({
						success: function(model, response) {
							XAUtil.blockUI('unblock');
							XAUtil.allowNavigation();
							XAUtil.notifySuccess('Success', localization.tt('msg.policyDeleteMsg'));
							App.appRouter.navigate("#!/service/"+that.rangerService.id+"/policies",{trigger: true});
						},
						error: function (model, response, options) {
							XAUtil.blockUI('unblock');
							if ( response && response.responseJSON && response.responseJSON.msgDesc){
								    XAUtil.notifyError('Error', response.responseJSON.msgDesc);
							    }else
							    	XAUtil.notifyError('Error', 'Error deleting Policy!');
							    console.log("error");
						}
					});
				}
			});
		},
		/** on close */
		onClose: function(){
			XAUtil.allowNavigation();
		}

	});

	return RangerPolicyCreate;
});
