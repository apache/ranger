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
    
	var XAEnums			= require('utils/XAEnums');
	var localization	= require('utils/XALangSupport');

	require('backbone-forms');
	require('backbone-forms.list');
	require('backbone-forms.templates');
	require('backbone-forms.XAOverrides');

	var ServiceForm = Backbone.Form.extend(
	/** @lends ServiceForm */
	{
		_viewName : 'ServiceForm',

    	/**
		* intialize a new ServiceForm Form View 
		* @constructs
		*/
		templateData: function(){
			var serviceDetail="", serviceConfig="";
			_.each(this.schema, function(obj, name){
			  if(!_.isUndefined(obj['class']) && obj['class'] == 'serviceConfig') 
			    serviceConfig += name+",";
			  else
			   serviceDetail += name+",";
			});
			return {
				serviceDetail : serviceDetail.slice(0,-1),
				serviceConfig : serviceConfig.slice(0,-1)
			};
		},
		initialize: function(options) {
			console.log("initialized a ServiceForm Form View");
			_.extend(this, _.pick(options, 'rangerServiceDefModel'));
    		Backbone.Form.prototype.initialize.call(this, options);

			this.bindEvents();
		},

		/** all events binding here */
		bindEvents : function(){
		},

		/** schema for the form
		* If you see that a field should behave similarly in all the forms, its 
		* better to make the change in this.model.schema itself
		*
		* Override here ONLY if special case!!
		*/

		fields: ['name', 'description', 'isEnabled', 'type','configs', '_vPassword'],

		schema : function(){

			var attrs = _.pick(_.result(this.rangerServiceDefModel,'schemaBase'), 'name', 'description', 'isEnabled', 'type');
			var that = this;
			_.each(this.rangerServiceDefModel.get('configs'),function(v,k){ 
				if (v != null) {

					var formObj = {};
					var enumObj = _.find(that.rangerServiceDefModel.get('enums'), function(e){ return e && e.name == v.type;});
					if (enumObj !== undefined ){
						formObj.type = 'Select';
						formObj.options = _.pluck(_.compact(enumObj.elements),'label'); 
					} else {
						switch(v.type){
							case 'string' : formObj.type = 'Text'; break;
							case 'bool' : formObj.type = 'Checkbox'; formObj.options = { y: 'Yes', n: 'No' }; break;
							case 'int' : formObj.type = 'Number'; break;
							default : formObj.type = 'Text'; break;
						}	


					}
					formObj.title = v.label || v.name;
					formObj.validators = [];
					if (_.has(v,'mandatory') && v.mandatory){
						formObj.validators.push('required');
						formObj.title = formObj.title +" *"
					}
					formObj['class'] = 'serviceConfig';
					var name = v.name;
					attrs[name] = formObj;
				}
			});
			return attrs;
		},

		/** on render callback */
		render: function(options) {
			Backbone.Form.prototype.render.call(this, options);

			this.initializePlugins();
			this.renderCustomFields();
			/*if(!this.model.isNew())
				this.fields.serviceType.editor.$el.prop('disabled',true);
			else
				this.fields.activeStatus.editor.setValue(XAEnums.ActiveStatus.STATUS_ENABLED.value);
			*/
			if(this.model.isNew())
				this.fields.isEnabled.editor.setValue(XAEnums.ActiveStatus.STATUS_ENABLED.value);
			else{
				//Set isEnabled Status
				if(!_.isUndefined(this.model.get('isEnabled')))
					if(XAEnums.ActiveStatus.STATUS_ENABLED.value == this.model.get('isEnabled'))
						this.fields.isEnabled.editor.setValue(XAEnums.ActiveStatus.STATUS_ENABLED.value);
					else
						this.fields.isEnabled.editor.setValue(XAEnums.ActiveStatus.STATUS_DISABLED.value);
			}
		},

		/** all custom field rendering */
		renderCustomFields: function(){
		},

		/** all post render plugin initialization */
		initializePlugins: function(){
		},

		formValidation : function(){
			//return false;
			return true;
		},

		beforeSave : function(){
			var that = this;
			//Set configs for service 
			var config = {};
			_.each(this.rangerServiceDefModel.get('configs'),function(obj){
				if(!_.isNull(obj)){
					config[obj.name] = that.model.get(obj.name).toString();
					that.model.unset(obj.name);
				}
			});
			//this.model.set('configs',JSON.stringify(config));
			this.model.set('configs',config);

			//Set service type
			this.model.set('type',this.rangerServiceDefModel.get('name'))
			/*_.each(XAEnums.AssetType, function(asset){
				if(asset.label.toUpperCase() == this.rangerServiceDefModel.get('name').toUpperCase())
					this.model.set('type',asset.label)
			},this);*/
//			
			//Set isEnabled
			if(parseInt(this.model.get('isEnabled')) == XAEnums.ActiveStatus.STATUS_ENABLED.value)
				this.model.set('isEnabled',true);
			else
				this.model.set('isEnabled',false);
			
			//Remove unwanted attributes from model
			if(!this.model.isNew()){
				_.each(JSON.parse(this.model.attributes.configs),function(value, name){
					this.model.unset(name)
				},this);
			}
		},

		removeElementFromArr : function(arr ,elem){
			var index = $.inArray(elem,arr);
			if(index >= 0) arr.splice(index,1);
			return arr;
		}
	});

	return ServiceForm;
});