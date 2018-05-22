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
	var localization	= require('utils/XALangSupport');
	var RangerServiceList 	= require('collections/RangerServiceList');
	var RangerService 		= require('models/RangerService');
	var ServicemanagerlayoutTmpl = require('hbs!tmpl/common/ServiceManagerLayout_tmpl');
	var vUploadServicePolicy		= require('views/UploadServicePolicy');
	var vDownloadServicePolicy		= require('views/DownloadServicePolicy');
        var RangerServiceViewDetail = require('views/service/RangerServiceViewDetail');
	require('Backbone.BootstrapModal');
	return Backbone.Marionette.Layout.extend(
	/** @lends Servicemanagerlayout */
	{
		_viewName : name,
		
    	template: ServicemanagerlayoutTmpl,

		templateHelpers: function(){
			return {
				operation 	: SessionMgr.isSystemAdmin() || SessionMgr.isKeyAdmin(),
				serviceDefs : this.collection.models,
				services 	: this.services.groupBy("type"),
                                showImportExportBtn : (SessionMgr.isUser() || XAUtil.isAuditorOrKMSAuditor(SessionMgr)) ? false : true
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
    		'downloadReport'      : '[data-id="downloadBtnOnService"]',
    		'uploadServiceReport' :'[data-id="uploadBtnOnServices"]',
    		'exportReport'      : '[data-id="exportBtn"]',
                'importServiceReport' :'[data-id="importBtn"]',
                'viewServices' : '[data-name="viewService"]'
    	},

		/** ui events hash */
		events : function(){
			var events = {};
			events['click ' + this.ui.btnDelete]	= 'onDelete';
			events['click ' + this.ui.downloadReport]	= 'downloadReport';
			events['click ' + this.ui.uploadServiceReport]	= 'uploadServiceReport';
			events['click ' + this.ui.exportReport]	= 'downloadReport';
			events['click ' + this.ui.importServiceReport]	= 'uploadServiceReport';
                        events['click ' + this.ui.viewServices]   = 'viewServices';
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
		downloadReport : function(e){
			var that = this;
			if(SessionMgr.isKeyAdmin()){
				if(this.services.length == 0){
					XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToExport'));
					return;
				}
			}
			var el = $(e.currentTarget), serviceType = el.attr('data-servicetype');
			if(serviceType){
				var componentServices = this.services.where({'type' : serviceType });
                    if(componentServices.length == 0 ){
	            	XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToExport'));
	            	return;
	            }
			}else{
				if(SessionMgr.isSystemAdmin()){
					if(location.hash == "#!/policymanager/resource"){
						var servicesList = _.omit(this.services.groupBy('type'),'tag','kms');
						if(_.isEmpty(servicesList)){
							XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToExport'));
							return;
						}
					}else{
						var servicesList = _.pick(this.services.groupBy('type'),'tag');
						if(_.isEmpty(servicesList)){
							XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToExport'));
							return;
						}
						
					}
				}
			}
			
			 var view = new vDownloadServicePolicy({
              	serviceType		:serviceType,
				collection 		: new Backbone.Collection([""]),
				serviceDefList	: this.collection,
				services		: this.services
			});
            var modal = new Backbone.BootstrapModal({
				content	: view,
				title	: 'Export Policy',
				okText  :"Export",
				animate : true
			}).open();
			
		},
		uploadServiceReport :function(e){
		    var that = this;
		    if(SessionMgr.isKeyAdmin()){
				if(this.services.length == 0){
					XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToImport'));
					return;
				}
			}
		    var el = $(e.currentTarget), serviceType = el.attr('data-servicetype');
			if(serviceType){
				var componentServices = this.services.where({'type' : serviceType });
                    if(componentServices.length == 0 ){
	            	XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToImport'));
	            	return;
	            }	
			}else{
				if(SessionMgr.isSystemAdmin()){
	            	if(location.hash=="#!/policymanager/resource"){
	                	var servicesList = _.omit(this.services.groupBy('type'),'tag','kms')
	                	if(_.isEmpty(servicesList)){
	                		XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToImport'));
	    					return;
	                	}
	                }else{
	                	var servicesList = _.pick(this.services.groupBy('type'),'tag')
	                	if(_.isEmpty(servicesList)){
	                		XAUtil.alertBoxWithTimeSet(localization.tt('msg.noServiceToImport'));
	    					return;
	                	}
	                	
	                }
	            }
				
			}
			var view = new vUploadServicePolicy({
                serviceType		: serviceType,
				collection 		: new Backbone.Collection([""]),
				serviceDefList	: this.collection,
				services		: this.services
			});	
			var modal = new Backbone.BootstrapModal({
				content	: view,	
				okText 	:"Import",
				title	: 'Import Policy',
				animate : true
			}).open();

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
                                                                if(!_.isUndefined(response) && !_.isUndefined(response.responseJSON) && !_.isUndefined(response.responseJSON.msgDesc && response.status !='419')){
									XAUtil.notifyError('Error', response.responseJSON.msgDesc);
								}
							}
						});
					}
				});
			}
		},
        viewServices : function(e){
            var that =this;
            var serviceId =  $(e.currentTarget).data('id');
            var rangerService = that.services.find(function(m){return m.id == serviceId});
            var serviceDef = that.collection.find(function(m){return m.get('name') == rangerService.get('type')});
            var view = new RangerServiceViewDetail({
                serviceDef : serviceDef,
                rangerService : rangerService,

            });
            var modal = new Backbone.BootstrapModal({
                animate : true,
                content     : view,
                title: localization.tt("h.serviceDetails"),
                okText :localization.tt("lbl.ok"),
                allowCancel : true,
                escape : true
            }).open();
            modal.$el.find('.cancel').hide();
        },
		/** on close */
		onClose: function(){
            XAUtil.removeUnwantedDomElement();
		}

	});
});
