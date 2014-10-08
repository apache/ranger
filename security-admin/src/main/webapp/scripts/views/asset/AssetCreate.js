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
 * Repository/Asset create view
 */

define(function(require){
    'use strict';

	var Backbone		= require('backbone');
	var App				= require('App');

	var XAUtil			= require('utils/XAUtils');
	var XAEnums			= require('utils/XAEnums');
	var XALinks 		= require('modules/XALinks');
	var localization	= require('utils/XALangSupport');

	var AssetForm		= require('views/asset/AssetForm');
	var AssetcreateTmpl = require('hbs!tmpl/asset/AssetCreate_tmpl');

	var AssetCreate = Backbone.Marionette.Layout.extend(
	/** @lends AssetCreate */
	{
		_viewName : 'AssetCreate',

		template: AssetcreateTmpl,
		
		templateHelpers : function(){
			return { editAsset : this.editAsset};
		},
        
		breadCrumbs :function(){
			if(this.model.isNew())
				return [XALinks.get('RepositoryManager'), XALinks.get('AssetCreate', {model:this.model})];
			else
				return [XALinks.get('RepositoryManager'), XALinks.get('AssetEdit',{model:this.model})];
		},        

		/** Layout sub regions */
		regions: {
			'rForm' :'div[data-id="r_form"]'
		},

		/** ui selector cache */
		ui: {
			'btnSave'	: '[data-id="save"]',
			'btnCancel' : '[data-id="cancel"]',
			'btnDelete' : '[data-id="delete"]',
			'btnTestConn' : '[data-id="testConn"]'
		},

		/** ui events hash */
		events: function() {
			var events = {};
			events['click ' + this.ui.btnSave]		= 'onSave';
			events['click ' + this.ui.btnCancel]	= 'onCancel';
			events['click ' + this.ui.btnDelete]	= 'onDelete';
			events['click ' + this.ui.btnTestConn]	= 'onTestConnection';
			return events;
		},

		/**
		 * intialize a new AssetCreate Layout 
		 * @constructs
		 */
		initialize: function(options) {
			console.log("initialized a AssetCreate Layout");

			_.extend(this, _.pick(options, 'repositoryName'));
			if(! this.model.isNew()){
				this.setupModel();
			}
			this.form = new AssetForm({
				model : this.model,
				template : require('hbs!tmpl/asset/AssetForm_tmpl')
			});
			this.editAsset = this.model.has('id') ? true : false;

			this.bindEvents();
		},
		setupModel : function(){
			var that = this;
			//var obj = _.pick(this.model.attributes,['username','password','fsDefaultName' ,'authorization', 'authentication', 'auth_to_local', 'datanode', 'namenode', 'secNamenode']);
			if(this.model.get('config')){
				var configObj = $.parseJSON(this.model.get('config')); 
				_.each(configObj,function(val,prop){
					_.each(that.model.propertiesNameMap,function(v,p){
						if(prop == v){
							that.model.set(p,val);
						}
					});
				});
			}
			
			//this.model.set('config',JSON.stringify(obj));
		},

		/** all events binding here */
		bindEvents : function(){
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
		},

		/** on render callback */
		onRender: function() {
			if(!this.editAsset){
				this.ui.btnDelete.hide();
				this.ui.btnSave.html('Add');
			}else{
				
			//	XAUtil.preventNavigation(localization.tt('dialogMsg.preventNavRepositoryForm'));
			}
			this.rForm.show(this.form);
			this.rForm.$el.dirtyFields();
			XAUtil.preventNavigation(localization.tt('dialogMsg.preventNavRepositoryForm'),this.rForm.$el);
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
			this.form.formValidation();
			this.saveAsset();

		},
		saveAsset : function(){
			var that = this;
			this.form.beforeSave();
			XAUtil.blockUI();
			this.model.save({},{
				wait: true,
				success: function () {
					XAUtil.blockUI('unblock');
					XAUtil.allowNavigation();
					var msg = that.editAsset ? 'Repository updated successfully' :'Repository created successfully';
					XAUtil.notifySuccess('Success', msg);
					
					if(that.editAsset){
						App.appRouter.navigate("#!/policymanager",{trigger: true});
						return;
					}
					
					App.appRouter.navigate("#!/policymanager",{trigger: true});
					
				},
				error: function (model, response, options) {
					XAUtil.blockUI('unblock');
					if ( response && response.responseJSON && response.responseJSON.msgDesc){
						if(response.responseJSON.msgDesc == "serverMsg.fsDefaultNameValidationError"){
							that.form.fields.fsDefaultName.setError(localization.tt(response.responseJSON.msgDesc));
							XAUtil.scrollToField(that.form.fields.fsDefaultName.$el);
						}else if(response.responseJSON.msgDesc == "Repository Name already exists"){
							response.responseJSON.msgDesc = "serverMsg.repositoryNameAlreadyExistsError";
							that.form.fields.name.setError(localization.tt(response.responseJSON.msgDesc));
							XAUtil.scrollToField(that.form.fields.name.$el);
						}else if(response.responseJSON.msgDesc == "XUser already exists"){
							response.responseJSON.msgDesc = "serverMsg.userAlreadyExistsError";
							that.form.fields.userName.setError(localization.tt(response.responseJSON.msgDesc));
							XAUtil.scrollToField(that.form.fields.userName.$el);
						}else
							XAUtil.notifyError('Error', response.responseJSON.msgDesc);
					}else
						XAUtil.notifyError('Error', 'Error creating Asset!');
					console.log("error");
				}
			});
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
							XAUtil.notifySuccess('Success', 'Repository delete successfully');
							App.appRouter.navigate("#!/policymanager",{trigger: true});
						},
						error: function (model, response, options) {
							XAUtil.blockUI('unblock');
							if ( response && response.responseJSON && response.responseJSON.msgDesc){
									XAUtil.notifyError('Error', response.responseJSON.msgDesc);
							}else
								XAUtil.notifyError('Error', 'Error occured while deleting asset!');
						}
					});
					
				}
			});
		},
		onTestConnection : function(){
			var errors = this.form.commit({validate : false});
			if(! _.isEmpty(errors)){
				return;
			}
			this.form.beforeSave();
			this.model.testConfig(this.model,{
					//wait: true,
					success: function (msResponse, options) {
						if(msResponse.statusCode){
							if(!_.isUndefined(msResponse) && _.isArray(msResponse.messageList) 
														  && !_.isUndefined(msResponse.messageList[0].message)){
								if(!_.isEmpty(msResponse.messageList[0].message) && msResponse.messageList[0].message != "\n"){
									bootbox.dialog('<b>'+msResponse.messageList[0].message+'</b>',	[{
										label: "Show More..",
										callback:function(e){
											console.log(e)
											if($(e.currentTarget).text() == 'Show More..'){
												var div = '<div class="showMore">'+msResponse.msgDesc+'</div>';
												$(e.delegateTarget).find('.modal-body').append(div)
												$(e.currentTarget).html('Show Less..')
											}else{
												$(e.delegateTarget).find('.showMore').remove();
												$(e.currentTarget).html('Show More..')
											}
											return false;
										}
									}, {
										label: "OK",
										callback:function(){}
									}]
									);
								}else{
									if(!_.isEmpty(msResponse.msgDesc))
										bootbox.alert(msResponse.msgDesc);
									else
										bootbox.alert("Connection Problem.");
								}
							}else{
								bootbox.alert("Connection Problem.");
							}
						}
						else
							bootbox.alert("Connected Successfully.");
					},
					error: function (msResponse, options) {
					}	
				});
		},
		onCancel : function(){
			XAUtil.allowNavigation();
			App.appRouter.navigate("#!/policymanager",{trigger: true});
		},
		/** on close */
		onClose: function(){
			XAUtil.allowNavigation();
		}
	});

	return AssetCreate; 
});
