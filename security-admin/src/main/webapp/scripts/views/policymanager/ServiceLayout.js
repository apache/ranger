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

	var Backbone			= require('backbone');
	var XALinks 			= require('modules/XALinks');
	var XAEnums 			= require('utils/XAEnums');
	var XAUtil				= require('utils/XAUtils');
	var SessionMgr 			= require('mgrs/SessionMgr');
	
	var RangerServiceList 	= require('collections/RangerServiceList');
	var RangerService 		= require('models/RangerService');
	
	var ServicemanagerlayoutTmpl = require('hbs!tmpl/common/ServiceManagerLayout_tmpl');
	return Backbone.Marionette.Layout.extend(
	/** @lends Servicemanagerlayout */
	{
		_viewName : name,
		
    	template: ServicemanagerlayoutTmpl,

		templateHelpers: function(){
			return {
				operation 	: SessionMgr.isSystemAdmin() || SessionMgr.isKeyAdmin(),
				serviceDefs : this.collection.models,
				services 	: this.services.groupBy("type")
			};
		},
    	breadCrumbs :function(){
    		if(this.type == "tag"){
    			return [XALinks.get('TagBasedServiceManager')];
    		}
    		return [XALinks.get('ServiceManager')];
    	},

		/** Layout sub regions */
    	regions: {},

    	/** ui selector cache */
    	ui: {
    		'btnDelete' : '.deleteRepo',
    	},

		/** ui events hash */
		events : function(){
			var events = {};
			events['click ' + this.ui.btnDelete]	= 'onDelete';
			return events;
		},
    	/**
		* intialize a new Servicemanagerlayout Layout 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a Servicemanagerlayout Layout");
			this.services = new RangerServiceList();	
			_.extend(this, _.pick(options, 'collection','type'));
			this.bindEvents();
			this.initializeServices();
		},

		/** all events binding here */
		bindEvents : function(){
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
			this.listenTo(this.collection, "sync", this.render, this);
			this.listenTo(this.collection, "request", function(){
				this.$('[data-id="r_tableSpinner"]').removeClass('display-none').addClass('loading');
			}, this);
		},

		/** on render callback */
		onRender: function() {
			this.$('[data-id="r_tableSpinner"]').removeClass('loading').addClass('display-none');
			this.initializePlugins();
		},
		/** all post render plugin initialization */
		initializePlugins: function(){

		},

		initializeServices : function(){
			this.services.setPageSize(100);
			this.services.fetch({
			   cache : false,
			   async : false
			});

		},
		onDelete : function(e){
			var that = this;
			var model = this.services.get($(e.currentTarget).data('id'));
			if(model){
				model = new RangerService(model.attributes);
				XAUtil.confirmPopup({
					msg :'Are you sure want to delete ?',
					callback : function(){
						XAUtil.blockUI();
						model.destroy({
							success: function(model, response) {
								XAUtil.blockUI('unblock');
								that.services.remove(model.get('id'));
								XAUtil.notifySuccess('Success', 'Service deleted successfully');
								that.render();
							},
							error :function(model, response) {
								XAUtil.blockUI('unblock');
								if(!_.isUndefined(response) && !_.isUndefined(response.responseJSON) && !_.isUndefined(response.responseJSON.msgDesc)){
									XAUtil.notifyError('Error', response.responseJSON.msgDesc);
								}
							}
						});
					}
				});
			}
		},
		/** on close */
		onClose: function(){
		}

	});
});
