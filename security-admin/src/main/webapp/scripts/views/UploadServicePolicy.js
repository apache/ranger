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
	var App				= require('App');
	var XAUtil			= require('utils/XAUtils');
	var XAEnums			= require('utils/XAEnums');
	var XALinks 		= require('modules/XALinks');
	var UploadservicepolicyTmpl = require('hbs!tmpl/common/uploadservicepolicy_tmpl');
	
	var ServiceMappingItem = Backbone.Marionette.ItemView.extend({
		_msvName : 'ServiceMappingItem',
		template : require('hbs!tmpl/common/ServiceMappingItem'),
		ui : { 
			sourceInput : 'input[data-id="source"]',
			destinationSelect : '[data-id="destination"]',
			deleteMap : 'a[data-id="delete"]',
			'overrridCheck'	: 'input[data-name="override"]:checked',
		},
		events : function(){
			var events = {};
			events['change ' + this.ui.sourceInput]	= 'onSourceChange';
			events['change ' + this.ui.destinationSelect]	= 'onDestinationSelect';
			events['click ' + this.ui.deleteMap]	= 'onDeleteMapClick';
			return events;
		},

		initialize : function(options) {
			_.extend(this, _.pick(options, 'collection','serviceNames','services'));
			
		},
		onSourceChange : function(e){
			var sourceValue = e.currentTarget.value.trim();
			this.model.set('source', _.isEmpty(sourceValue) ? undefined : sourceValue);
		},
		onDestinationSelect : function(e) {
		   this.model.set('destination', _.isEmpty(e.currentTarget.value) ? undefined : e.currentTarget.value);
		   var serviceTypes = _.find( this.services.models , function(m){
			   return m.get('name') == e.currentTarget.value
		   });
		   if(!_.isUndefined(serviceTypes)){
			   this.model.set('serviceType' , serviceTypes.get('type') );
		   }else{
			   this.model.set('serviceType' , " " );
		   }
		},
		onDeleteMapClick : function(){
			this.collection.remove(this.model)	
		},
 
		onRender : function() {
			var that = this;
			var options = _.map(this.serviceNames, function(m, key){ return { 'id' : m.name, 'text' : m.name}; });
			this.ui.sourceInput.val(this.model.get('source'));
			this.ui.destinationSelect.val(this.model.get('destination'));
			this.ui.destinationSelect.select2({
				closeOnSelect: true,
				placeholder: 'Select service name',
			    width: '220px',
			    allowClear: true,
			    data:options,
			});
		}
	});
	
	var UploadServicePolicy = Backbone.Marionette.CompositeView.extend({
		
		template : UploadservicepolicyTmpl,
		templateHelpers : function(){
			return { 'serviceType' : this.serviceType };
		},
		getItemView : function(item){
			if(!item){
				return;
			}
			return ServiceMappingItem;
		},
		itemViewContainer : ".js-serviceMappingItems",
		itemViewOptions : function() {
			return {
				'collection' 	: this.collection,
				'serviceNames' 	: this.serviceNames,
				'services': this.services,
			};
		},
		initialize: function(options) {
		  this.bind("ok", this.okClicked);
		  _.extend(this, _.pick(options, 'collection','serviceNames','serviceDefList','serviceType','services'));
		  this.componentServices = this.services.where({'type' : this.serviceType })
		  this.serviceNames =this.componentServices.map(function(m){ return { 'name' : m.get('name') } });
		},
		ui:{
			'importFilePolicy'  : '[data-id="uploadPolicyFile"]',
			'addServiceMaping'	: '[data-id="addServiceMaping"]',
			'componentType'		: '[data-id="componentType"]',
			'fileNameClosebtn' 	: '[data-id="fileNameClosebtn"]'
		},
		events: function() {
			var events = {};
			events['change ' + this.ui.importFilePolicy] = 'importPolicy';
			events['click ' + this.ui.addServiceMaping] = 'onAddClick';
			events['click ' + this.ui.fileNameClosebtn]	= 'fileNameClosebtn';
			return events;
		},
		okClicked: function (modal) {
			if( _.isUndefined(this.targetFileObj) || (_.isEmpty(this.ui.componentType.val()) && this.ui.componentType.is(":visible"))){
				if(_.isUndefined(this.targetFileObj)){
					this.$el.find('.selectFileValidationMsg').show();
				}else{
					this.$el.find('.selectFileValidationMsg').hide();
				}
				if (_.isEmpty(this.ui.componentType.val())){
					this.$el.find('.seviceFiledValidationFile').show();
				}else{
					this.$el.find('.seviceFiledValidationFile').hide();
				}
				return modal.preventClose();
			}
			var that = this, serviceMapping = {}, fileObj = this.targetFileObj, preventModal = false , url ="";
			if(this.$el.find('input[data-name="override"]').is(':checked')){
        	    url = "service/plugins/policies/importPoliciesFromFile?isOverride=true";
			}else{
        	    url = "service/plugins/policies/importPoliciesFromFile?isOverride=false";
			}
			this.collection.each(function(m){
				if( m.get('source') !== undefined && m.get('destination') == undefined 
						|| m.get('source') == undefined && m.get('destination') !== undefined ){
					that.$el.find('.serviceMapErrorMsg').show();
					that.$el.find('.serviceMapTextError').hide();
					preventModal = true;
				}
				if(!_.isUndefined(m.get('source'))){
					serviceMapping[m.get('source')] = m.get('destination') 
				}
			});
			if(preventModal){
				modal.preventClose();
				return;
			}
			if(this.collection.length>1){
				that.collection.models.some(function(m){
					   if (!_.isEmpty(m.attributes)) {
	                        if (m.has('source') && m.get('source') != '') {
	                            var model = that.collection.where({
	                                'source': m.get('source')
	                            });
	                            if (model.length > 1) {
	                            	that.$el.find('.serviceMapTextError').show();
	                            	that.$el.find('.serviceMapErrorMsg').hide();
	                            	preventModal = true;
	                                return true;
	                            }
	                        }
					   }
				})
			}
			if(preventModal){
				modal.preventClose();
				return;
			}
			this.formData = new FormData();
	        this.formData.append('file', fileObj);
	        if(!_.isEmpty(serviceMapping)){ 
	        	this.formData.append('servicesMapJson', new Blob([JSON.stringify(serviceMapping)],{type:'application/json'}));
	        }
		var compString = ''
	        if(!_.isUndefined(that.serviceType)){
	        	compString=that.serviceType
	        }else{
	        	compString = this.ui.componentType.val()
	        }
	        XAUtil.blockUI();
		   	$.ajax({
		        type: 'POST',
		        url: url+"&serviceType="+compString,
		        enctype: 'multipart/form-data',
		        data: this.formData,
		        cache: false,
		        dataType:'Json',
		        contentType: false,
		        processData: false,
		        success: function () {
		        	XAUtil.blockUI('unblock');
		        	var msg =  'File import successfully.' ;
					XAUtil.notifySuccess('Success', msg);
	
		        },
		   	      error : function(response,model){
		   	    	XAUtil.blockUI('unblock');
		   	 	if ( response && response.responseJSON && response.responseJSON.msgDesc){
					if(response.status == '419'){
						XAUtil.defaultErrorHandler(model,response);
					}else{
						XAUtil.notifyError('Error', response.responseJSON.msgDesc);
					}
				} else {
			       	XAUtil.notifyError('Error', 'File import failed.');
				}
			      }
		    });
	    },
	    onAddClick : function(){
	    	this.collection.add(new Backbone.Model());
	    },
	 	onRender: function() {
	 		this.$el.find('.fileValidation').hide();
        	this.$el.find('.selectFileValidationMsg').hide();
        	if(this.serviceType==undefined){
			   this.$el.find('.seviceFiled').show();
			   this.renderComponentSelect();
        	}else{
			   this.$el.find('.seviceFiled').hide();
        	}
		},
		/* add 'component' and 'policy type' select */
		renderComponentSelect: function(){
			var that = this;
			var options = this.serviceDefList.map(function(m){ return { 'id' : m.get('name'), 'text' : m.get('name')}; });
			var optionVal = options.map(function(m){return m.text})
            this.ui.componentType.val(optionVal);
			this.ui.componentType.select2({
				multiple: true,
				closeOnSelect: true,
				placeholder: 'Select Component',
			    width: '530px',
			    allowClear: true,
			    data: options
			}).on('change', function(e){
				var selectedComp  = e.currentTarget.value, componentServices = [];
				_.each(selectedComp.split(","), function(type){
					var services = that.services.where({'type' : type });
					componentServices = componentServices.concat(services);
				});
				var names = componentServices.map(function(m){ return { 'name' : m.get('name') } });
				that.serviceNames = names;
				if(!_.isUndefined(e.removed)){
					_.each(that.collection.models , function(m){
						if(m.get('serviceType') == e.removed.id){
							var mapModels = that.collection.filter(function(m){
								return m.get('serviceType') == e.removed.id;
							})
							if(!_.isUndefined(mapModels)){
								that.collection.remove(mapModels);
							}
						}
					});
				}
				that.collection.trigger('reset');
			}).trigger('change');
		},
		importPolicy : function(e){
			var that =this;
			console.log("uploading....");
			this.$el.find('.selectFile').hide(); 
			this.$el.find('.selectFileValidationMsg').hide(); 
			this.$el.find('.fileValidation').hide();
			this.targetFileObj = e.target.files[0];
			if(!_.isUndefined(this.targetFileObj)){
				this.$el.find('.selectFile').html('<i>'+this.targetFileObj.name+'</i><label class="icon icon-remove icon-1x icon-remove-btn" data-id="fileNameClosebtn"></label>').show()
			}else{
				this.$el.find('.selectFile').html("No file chosen").show();
			}
		},
		fileNameClosebtn : function(){
            this.$el.find('.selectFile').hide()
	     	this.$el.find('.selectFile').html("No file chosen").show()
			this.$el.find('.fileValidation').hide();
			this.$el.find('.selectFileValidationMsg').hide();
			this.targetFileObj = undefined;
			this.ui.importFilePolicy.val('');
 		}
		
	});
	return UploadServicePolicy; 
});
