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
	var App				= require('App');
	var XAUtil			= require('utils/XAUtils');
	var XAEnums			= require('utils/XAEnums');
	var XALinks 		= require('modules/XALinks');
	var localization	= require('utils/XALangSupport');
	var VPasswordChange	= require("models/VXPasswordChange");
	
	var UserProfileForm = require('views/user/UserProfileForm');
	var UserprofileTmpl = require('hbs!tmpl/user/UserProfile_tmpl');

	var UserProfile = Backbone.Marionette.Layout.extend(
	/** @lends UserProfile */
	{
		_viewName : 'UserProfile',
		
    	template: UserprofileTmpl,
        breadCrumbs : [XALinks.get('UserProfile')],
		/** Layout sub regions */
    	regions: {
    		'rForm' :'div[data-id="r_form"]'
    	},

    	/** ui selector cache */
    	ui: {
    		tab 		: '.nav-tabs',
    		saveBtn 	: 'button[data-id="save"]',
    		cancelBtn 	: 'button[data-id="cancel"]'
    	},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			events['click '+this.ui.tab+' li a']  = 'onTabChange';
			events['click ' + this.ui.saveBtn]  = 'onSave';
			events['click ' + this.ui.cancelBtn]  = 'onCancel';
			return events;
		},

    	/**
		* intialize a new UserProfile Layout 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a UserProfile Layout");
			//_.extend(this, _.pick(options, ''));
			this.showBasicFields = true;
			this.bindEvents();
		},

		/** all events binding here */
		bindEvents : function(){
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
		},

		/** on render callback */
		onRender: function() {
		/*	if(!this.model.isNew()){
				this.$('[data-tab="edit-password"]').hide();
				this.$('[data-tab="edit-basic"]').hide();
			}*/
			if(!_.isUndefined(this.model.get('userSource')) && this.model.get('userSource') == XAEnums.UserSource.XA_USER.value){
				this.$('[data-tab="edit-password"]').hide();
				this.$('[data-tab="edit-basic"]').hide();
				this.ui.saveBtn.hide();
			}
			this.initializePlugins();
			this.renderForm();
		},
		renderForm : function(){
			this.form = new UserProfileForm({
				template :require('hbs!tmpl/user/UserProfileForm_tmpl'),
				model : this.model,
				showBasicFields : this.showBasicFields
			});
			this.rForm.show(this.form);
		},
		/** all post render plugin initialization */
		initializePlugins: function(){
		},
		onTabChange : function(e){
			this.showBasicFields = $(e.currentTarget).parent().data('tab') == 'edit-basic' ? true : false;
			this.renderForm();
			this.clearPasswordFields();
		},
		onSave : function(){
			var errors = this.form.commit({validate : false});
			if(! _.isEmpty(errors)){
				return;
			}
			this.form.afterCommit();
			if(this.showBasicFields){
				this.saveUserDetail();
			}else{
				this.savePasswordDetail();
			}
		},
		saveUserDetail : function(){
			this.model.saveUserProfile(this.model,{
				wait: true,
				success: function () {
					XAUtil.notifySuccess('Success', "User profile updated successfully !!");
					App.appRouter.navigate("#!/policymanager",{trigger: true});
					Communicator.vent.trigger('ProfileBar:rerender');
					
					//console.log("success");
				},
				error: function (model, response, options) {
					if(model.responseJSON != undefined && _.isArray(model.responseJSON.messageList)){
						if(model.responseJSON.messageList[0].name == "INVALID_INPUT_DATA"){
							if (model.responseJSON.msgDesc == "Validation failure"){
								XAUtil.notifyError('Error', "Please try different name.");
								return;
							}
							else{
								XAUtil.notifyError('Error', model.responseJSON.msgDesc);
								return;
							}
						}
						
					}
					if ( response && response.responseJSON && response.responseJSON.msgDesc){
						that.form.fields.name.setError(response.responseJSON.msgDesc);
						XAUtil.notifyError('Error', response.responseJSON.msgDesc);
					}else
						XAUtil.notifyError('Error', 'Error occurred while updating user profile!!');
					//console.log("error");
				}
			});
		},
		savePasswordDetail : function(){
			var that = this;
			var vPasswordChange = new VPasswordChange();
			vPasswordChange.set({
				loginId : this.model.get('id'),
				emailAddress :this.model.get('emailAddress'), 
				oldPassword : this.model.get('oldPassword'),
				updPassword : this.model.get('newPassword')
			});
			this.model.changePassword(this.model.get('id'),vPasswordChange,{
				wait: true,
				success: function () {
					XAUtil.notifySuccess('Success', "User profile updated successfully !!");
					App.appRouter.navigate("#!/policymanager",{trigger: true});
					that.clearPasswordFields();
					console.log("success");
					
				},
				error: function (msResponse, options) {
					console.log("error occured during updated user profile: ",msResponse.response);
					XAUtil.notifyInfo('',localization.tt('msg.myProfileError'));
					if(localization.tt(msResponse.responseJSON.msgDesc) == "Invalid new password"){
						that.form.fields.newPassword.setError(localization.tt('validationMessages.newPasswordError'));
						that.form.fields.reEnterPassword.setError(localization.tt('validationMessages.newPasswordError'));
					}else if(localization.tt(msResponse.responseJSON.msgDesc) == " You can not use old password. "){
						that.form.fields.oldPassword.setError(localization.tt('validationMessages.oldPasswordRepeatError'));
					}	
					else{
						that.form.fields.oldPassword.setError(localization.tt('validationMessages.oldPasswordError'));
						
					}
					//that.form.fields.name.setError(response.responseJSON.msgDesc);
//					XAUtil.notifyError('Error', response.responseJSON.msgDesc);
					//that.form.fields.newPassword.setError(msResponse.responseJSON.msgDesc)
//					that.clearPasswordFields();
					console.log("error");
				}
			});
		}, 
		clearPasswordFields : function(){
			this.form.fields.oldPassword.setValue('');
			this.form.fields.newPassword.setValue('');
			this.form.fields.reEnterPassword.setValue('');
		},
		onCancel : function(){
//			App.appRouter.navigate("",{trigger: false});
			window.history.back();
		},
		/** on close */
		onClose: function(){
		}

	});

	return UserProfile; 
});
