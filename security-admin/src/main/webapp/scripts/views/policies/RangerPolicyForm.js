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
	var XAUtil			= require('utils/XAUtils');
    
	var VXAuditMap		= require('models/VXAuditMap');
	var VXPermMap		= require('models/VXPermMap');
	var VXPermMapList	= require('collections/VXPermMapList');
	var VXGroupList		= require('collections/VXGroupList');
	var VXAuditMapList	= require('collections/VXAuditMapList');
	var VXUserList		= require('collections/VXUserList');
	var PermissionList 	= require('views/policies/PermissionList');
	var RangerPolicyResource		= require('models/RangerPolicyResource');
	var BackboneFormDataType	= require('models/BackboneFormDataType');

	require('backbone-forms.list');
	require('backbone-forms.templates');
	require('backbone-forms');
	require('backbone-forms.XAOverrides');
	require('jquery-ui');
	require('tag-it');

	var RangerPolicyForm = Backbone.Form.extend(
	/** @lends RangerPolicyForm */
	{
		_viewName : 'RangerPolicyForm',

    	/**
		* intialize a new RangerPolicyForm Form View 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a RangerPolicyForm Form View");
			_.extend(this, _.pick(options, 'rangerServiceDefModel', 'rangerService'));
    		this.setupForm();
    		Backbone.Form.prototype.initialize.call(this, options);

			this.initializeCollection();
			this.bindEvents();
		},
		initializeCollection: function(){
			this.formInputList 		= XAUtil.makeCollForGroupPermission(this.model);
		},
		/** all events binding here */
		bindEvents : function(){
			this.on('isAuditEnabled:change', function(form, fieldEditor){
    			this.evAuditChange(form, fieldEditor);
    		});
			this.on('isEnabled:change', function(form, fieldEditor){
				this.evIsEnabledChange(form, fieldEditor);
			});
		},

		/** fields for the form
		*/
		fields: ['name', 'description', 'isEnabled', 'isAuditEnabled'],
		schema :function(){
			return this.getSchema();
		},
		getSchema : function(){
			var attrs = {};
			var basicSchema = ['id', 'name','isEnabled']
			var schemaNames = ['description', 'isAuditEnabled'];
			if(this.model.isNew()){
				basicSchema.shift();
			}
			
			var formDataType = new BackboneFormDataType();
			attrs = formDataType.getFormElements(this.rangerServiceDefModel.get('resources'),this.rangerServiceDefModel.get('enums'), attrs, this);
			
			var attr1 = _.pick(_.result(this.model,'schemaBase'),basicSchema);
			var attr2 = _.pick(_.result(this.model,'schemaBase'),schemaNames);
			return _.extend(attr1,_.extend(attrs,attr2));
		},
		/** on render callback */
		render: function(options) {
			var that = this;
			
			Backbone.Form.prototype.render.call(this, options);
			//initialize path plugin for hdfs component : resourcePath
			if(!_.isUndefined(this.initilializePathPlugin) && this.initilializePathPlugin){ 
				this.initializePathPlugins();
			}
			this.renderCustomFields();
			if(!this.model.isNew()){
				this.setUpSwitches();
			}
			this.$el.find('.field-isEnabled').find('.control-label').remove();
		},
		evAuditChange : function(form, fieldEditor){
			XAUtil.checkDirtyFieldForToggle(fieldEditor.$el);
		},
		evIsEnabledChange : function(form, fieldEditor){
			XAUtil.checkDirtyFieldForToggle(fieldEditor.$el);
		},
		setupForm : function() {
			if(!this.model.isNew()){
				_.each(this.model.get('resources'),function(obj,key){
					var resourceDef = _.findWhere(this.rangerServiceDefModel.get('resources'),{'name':key})
					var sameLevelResourceDef = _.where(this.rangerServiceDefModel.get('resources'), {'level': resourceDef.level});
					if(sameLevelResourceDef.length > 1){
						obj['resourceType'] = key;
						this.model.set('sameLevel'+resourceDef.level, obj)
					}else{
						this.model.set(resourceDef.name, obj)
					}
				},this)
			}
		},
		setUpSwitches :function(){
			var that = this;
			this.fields.isAuditEnabled.editor.setValue(this.model.get('isAuditEnabled'));
			this.fields.isEnabled.editor.setValue(this.model.get('isEnabled'));
			
		},
		/** all custom field rendering */
		renderCustomFields: function(){
			var that = this;
			var accessType = this.rangerServiceDefModel.get('accessTypes').filter(function(val) { return val !== null; });
			this.userList = new VXUserList();
			var params = {sortBy : 'name'};
			this.userList.setPageSize(100,{fetch:false});
			this.userList.fetch({
				cache :true,
				data: params,
				async : false
			});
			this.groupList = new VXGroupList();
			this.groupList.setPageSize(100,{fetch:false});
			this.groupList.fetch({
					cache :true,
					data : params
				}).done(function(){
					that.$('[data-customfields="groupPerms"]').html(new PermissionList({
						collection : that.formInputList,
						groupList  : that.groupList,
						userList   : that.userList,
						model 	   : that.model,
						accessTypes: accessType,
						rangerServiceDefModel : that.rangerServiceDefModel
					}).render().el);
			});

		},
	
		beforeSave : function(){
			var that = this, resources = [];

			var resources = {};
			_.each(this.rangerServiceDefModel.get('resources'),function(obj){
				if(!_.isNull(obj)){
					var rPolicyResource = new RangerPolicyResource();
					var tmpObj =  that.model.get(obj.name);
					if(!_.isUndefined(tmpObj) && _.isObject(tmpObj)){
						rPolicyResource.set('values',tmpObj.resource.split(','));
						if(!_.isUndefined(tmpObj.isRecursive)){
							rPolicyResource.set('isRecursive', tmpObj.isRecursive)
						}
						if(!_.isUndefined(tmpObj.isExcludes)){
							rPolicyResource.set('isExcludes', tmpObj.isExcludes)
						}
					}
					if(rPolicyResource.has('values')){
						if(!_.isUndefined(tmpObj) && !_.isUndefined(tmpObj.resourceType)){
							resources[tmpObj.resourceType] = rPolicyResource;
						}else{
							if(_.isUndefined(resources[obj.name])){
								resources[obj.name] = rPolicyResource;
							}
						}
					}
					that.model.unset(obj.name);
				}
			});
			this.model.set('resources',resources);
			this.model.unset('isRecursive');
			this.model.unset('path');
			
			//Set UserGroups Permission
			
			var RangerPolicyItem = Backbone.Collection.extend();
			var policyItemList = new RangerPolicyItem();
			policyItemList = this.setPermissionsToColl(this.formInputList, policyItemList);
			
			this.model.set('policyItems', policyItemList)
			this.model.set('service',this.rangerService.get('name'));			
			/*//Unset attrs which are not needed 
			_.each(this.model.attributes.resources,function(obj,key){
				this.model.unset(key, obj.values.toString())
			},this)*/
			
		},
		setPermissionsToColl : function(list, policyItemList) {
			list.each(function(m){
				if(!_.isUndefined(m.get('groupName')) || !_.isUndefined(m.get("userName"))){ //groupName or userName
					var RangerPolicyItem=Backbone.Model.extend()
					var policyItem = new RangerPolicyItem();
					if(!_.isUndefined(m.get('groupName')) && !_.isNull(m.get('groupName'))){
						policyItem.set("groups",m.get("groupName").split(','));
					}
					if(!_.isUndefined(m.get('userName')) && !_.isNull(m.get('userName'))){
						policyItem.set("users",m.get("userName").split(','));
					}
					if(!_.isUndefined(m.get('delegateAdmin'))){
						policyItem.set("delegateAdmin",m.get("delegateAdmin"));
					}
					
					var RangerPolicyItemAccessList = Backbone.Collection.extend();
					var rangerPlcItemAccessList = new RangerPolicyItemAccessList(m.get('accesses'));
					policyItem.set('accesses', rangerPlcItemAccessList)
					
					if(!_.isUndefined(m.get('conditions'))){
						var RangerPolicyItemConditionList = Backbone.Collection.extend();
						var rPolicyItemCondList = new RangerPolicyItemConditionList(m.get('conditions'))
						policyItem.set('conditions', rPolicyItemCondList)
					}
					policyItemList.add(policyItem)
					
				}
			}, this);
			return policyItemList;
		},
		/** all post render plugin initialization */
		initializePathPlugins: function(){
			var that= this,defaultValue = [];
			if(!this.model.isNew() && _.isUndefined(this.model.get('path'))){
				defaultValue = this.model.get('path').values;
			}
			function split( val ) {
				return val.split( /,\s*/ );
			}
			function extractLast( term ) {
				return split( term ).pop();
			}

			this.fields[that.pathFieldName].editor.$el.find('[data-js="resource"]').bind( "keydown", function( event ) {
				// don't navigate away from the field on tab when selecting an item
				/*if ( event.keyCode === $.ui.keyCode.TAB && $( this ).data( "ui-autocomplete" ).menu.active ) {
					event.preventDefault();
				}
				//TODO FIXME This is not working. We need a way so that when user enters  and presses ENTER
				// the text box should contain /app/billing* . Currently the '*' is getting removed.
				if ( event.keyCode === $.ui.keyCode.ENTER ) {
					event.preventDefault();
					event.stopPropagation();
					$(this).tagit("createTag", "brand-new-tag");
					//$(this).autocomplete('close');
					//$(this).val($(this).val() + ', ');
					
				}*/
			}).tagit({
				autocomplete : {
					cache: false,
					source: function( request, response ) {
						var url = "service/plugins/services/lookupResource/"+that.rangerService.get('name');
						var context ={
							'userInput' : extractLast( request.term ),
							'resourceName' : null,
							'resources' : { null:null }
						};
						var p = $.ajax({
							url : url,
							type : "POST",
							data : JSON.stringify(context),
							dataType : 'json',
							contentType: "application/json; charset=utf-8",
						}).done(function(data){
							if(data){
								response(data);
							} else {
								response();
							}

						}).error(function(){
							response();

						});
						setTimeout(function(){ 
							p.abort();
							console.log('connection timeout for resource path request...!!');
						}, 7000);
					},
					open : function(){
						$(this).removeClass('working');
					},
					search: function() {
						if(!_.isUndefined(this.value) && _.contains(this.value,',')){ 
							_.each(this.value.split(',') , function(tag){
								that.fields[that.pathFieldName].editor.$el.tagit("createTag", tag);
							});
				        	return false;
				        }	
						var term = extractLast( this.value );
						$(this).addClass('working');
						if ( term.length < 1 ) {
							return false;
						}
					},
					focus: function(event, ui) {
						var terms = split( this.value );
						terms.pop();
						terms.push( ui.item.value );
						this.value = terms.join( ", " );
						return false;
					},
					select: function( event, ui ) {
						var terms = split( this.value );
						terms.pop();
						terms.push( ui.item.value );
						terms.push( "" );
						this.value = terms.join( ", " );
						return false;
					}
					
				},
				beforeTagAdded: function(event, ui) {
			        // do something special
					that.fields[that.pathFieldName].$el.removeClass('error');
		        	that.fields[that.pathFieldName].$el.find('.help-inline').html('');
					var tags =  [];
			        console.log(ui.tag);
				if(ui.tagLabel.lastIndexOf('/') < 0 || 
			        		ui.tagLabel.lastIndexOf('/') == ui.tagLabel.length -1 && ui.tagLabel.lastIndexOf('/') != 0){
			        	tags = ui.tagLabel.substr(0,ui.tagLabel.lastIndexOf('/'));
			        	that.fields[that.pathFieldName].$el.addClass('error');
			        	that.fields[that.pathFieldName].$el.find('.help-inline').html('Please enter valid resource path : ' + ui.tagLabel);
			        	return false;
			        }
					}
			}).on('change',function(e){
				//check dirty field for tagit input type : `path`
				XAUtil.checkDirtyField($(e.currentTarget).val(), defaultValue.toString(), $(e.currentTarget))
			});
	
			
		},
		getPlugginAttr :function(autocomplete, options){
			var that =this;
			var type = options.containerCssClass;
			if(!autocomplete)
				return{tags : true,width :'220px',multiple: true,minimumInputLength: 1, 'containerCssClass' : type};
			else {
				
				
				return {
					containerCssClass : options.containerCssClass,
					closeOnSelect : true,
					tags:true,
					multiple: true,
					minimumInputLength: 1,
					width :'220px',
					tokenSeparators: [",", " "],
					initSelection : function (element, callback) {
						var data = [];
						$(element.val().split(",")).each(function () {
							data.push({id: this, text: this});
						});
						callback(data);
					},
					createSearchChoice: function(term, data) {
						if ($(data).filter(function() {
							return this.text.localeCompare(term) === 0;
						}).length === 0) {
							return {
								id : term,
								text: term
							};
						}
					},
					ajax: {
						url: options.lookupURL,
						type : 'POST',
						params : {
							timeout: 3000,
							contentType: "application/json; charset=utf-8",
						},
						cache: false,
						data: function (term, page) {
//							return _.extend(that.getDataParams(type, term));
							var context ={
									'userInput' : term,
									'resourceName' : null,
									'resources' : { null:null }
								};
							return JSON.stringify(context);
							
						},
						results: function (data, page) { 
							var results = [];
							if(data.length > 0){
								results = data.map(function(m, i){	return {id : m, text: m};	});
							}
							/*if(!_.isUndefined(data)){
								if(data.resultSize != "0"){
									results = data.vXStrings.map(function(m, i){	return {id : m.value, text: m.value};	});
								}
							}*/
							return { 
								results : results
							};
						},
						transport: function (options) {
							$.ajax(options).error(function() { 
								console.log("ajax failed");
								this.success({
									resultSize : 0
								});
							});
						}

					},	
					formatResult : function(result){
						return result.text;
					},
					formatSelection : function(result){
						return result.text;
					},
					formatNoMatches : function(term){
						switch (type){
							case  that.type.DATABASE :return localization.tt("msg.enterAlteastOneCharactere");
							case  that.type.TABLE :return localization.tt("msg.enterAlteastOneCharactere");
							case  that.type.COLUMN :return localization.tt("msg.enterAlteastOneCharactere");
							default : return "No Matches found";
						}
					}
				};	
			}
		},
		evResourceTypeChange : function(form, fieldEditor){
			var that = this;
			var name = fieldEditor.$el.val();
			var sameLevel = _.findWhere(this.sameLevelType, {'name' : fieldEditor.key});
			form.$el.find('[data-editors="'+sameLevel.options+'"]').children().hide();
			this.fields[name].editor.$el.select2('val','');
			this.fields[name].editor.$el.show();
			form.$el.find('.'+name).show();
			
			_.each(sameLevel.options.split(','), function(nm){
				if(name != nm){
					var index = this.fields.database.editor.validators.indexOf("required");
					this.fields[nm].editor.validators.splice(index,1);
					this.fields[nm].editor.$el.select2('val','');
				}
			}, this);
			
		},
	});

	return RangerPolicyForm;
});
