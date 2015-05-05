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
		templateData : function(){
			return { 'id' : this.model.id };
		},
		initialize: function(options) {
			console.log("initialized a RangerPolicyForm Form View");
			_.extend(this, _.pick(options, 'rangerServiceDefModel', 'rangerService'));
    		this.setupForm();
    		Backbone.Form.prototype.initialize.call(this, options);

			this.initializeCollection();
			this.bindEvents();
			this.defaultValidator={}
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
			this.on('policyForm:parentChildHideShow',this.renderParentChildHideShow);
		},

		/** fields for the form
		*/
		fields: ['name', 'description', 'isEnabled', 'isAuditEnabled'],
		schema :function(){
			return this.getSchema();
		},
		getSchema : function(){
			var attrs = {};
			var basicSchema = ['name','isEnabled']
			var schemaNames = ['description', 'isAuditEnabled'];
			
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
				this.initializePathPlugins(this.pathPluginOpts);
			}
			this.renderCustomFields();
			if(!this.model.isNew()){
				this.setUpSwitches();
			}
			//checkParent
			this.renderParentChildHideShow();
			
			//to show error msg on below the field(only for policy name)
			this.fields.isEnabled.$el.find('.control-label').removeClass();
			this.fields.name.$el.find('.help-inline').removeClass('help-inline').addClass('help-block margin-left-5')
		},
		evAuditChange : function(form, fieldEditor){
			XAUtil.checkDirtyFieldForToggle(fieldEditor.$el);
		},
		evIsEnabledChange : function(form, fieldEditor){
			XAUtil.checkDirtyFieldForToggle(fieldEditor.$el);
		},
		setupForm : function() {
			if(!this.model.isNew()){
				this.selectedResourceTypes = {};
				_.each(this.model.get('resources'),function(obj,key){
					var resourceDef = _.findWhere(this.rangerServiceDefModel.get('resources'),{'name':key})
					var sameLevelResourceDef = _.where(this.rangerServiceDefModel.get('resources'), {'level': resourceDef.level});
					if(sameLevelResourceDef.length > 1){
						obj['resourceType'] = key;
						this.model.set('sameLevel'+resourceDef.level, obj)
						//parentShowHide
						this.selectedResourceTypes['sameLevel'+resourceDef.level]=key;
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
			var params = {sortBy : 'name', isVisible : XAEnums.VisibilityStatus.STATUS_VISIBLE.value};
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
		renderParentChildHideShow : function(onChangeOfSameLevelType) {
			var formDiv = this.$el.find('.policy-form');
			if(!this.model.isNew() && !onChangeOfSameLevelType){
				_.each(this.selectedResourceTypes, function(val, sameLevelName) {
					if(formDiv.find('.field-'+sameLevelName).length > 0){
						formDiv.find('.field-'+sameLevelName).attr('data-name','field-'+val)
					}
				});
			}
			//hide form fields if it's parent is hidden
			var resources = formDiv.find('.control-group');
			_.each(resources, function(rsrc){ 
				var parent = $(rsrc).attr('parent')
				if( !_.isUndefined(parent) && ! _.isEmpty(parent)){
					var selector = "div[data-name='field-"+parent+"']"
					if(formDiv.find(selector).length > 0 && !formDiv.find(selector).hasClass('hideResource')){
							$(rsrc).removeClass('hideResource');
					}else{
						$(rsrc).addClass('hideResource');
					}
				}
			},this);
			//remove validation of fields if it's hidden
			_.each(this.fields, function(obj, key){
				if(obj.$el.hasClass('hideResource')){
					if($.inArray('required',obj.editor.validators) >= 0){
						this.defaultValidator[key] = obj.editor.validators;
						obj.editor.validators=[];
						var label = obj.$el.find('label').html();
						obj.$el.find('label').html(label.replace('*', ''));
					}
				}else{
					if(!_.isUndefined(this.defaultValidator[key])){
						obj.editor.validators = this.defaultValidator[key];
						if($.inArray('required',obj.editor.validators) >= 0){
							var label = obj.$el.find('label').html();
							obj.$el.find('label').html(label+"*");
						}
					}
				}
			}, this);
		},
		beforeSave : function(){
			var that = this, resources = [];

			var resources = {};
			//set sameLevel fieldAttr value with resource name
			_.each(this.model.attributes, function(val, key) {
                if(key.indexOf("sameLevel") >= 0){ 
                	this.model.set(val.resourceType,val);
                	that.model.unset(key);
                }
			},this);
			_.each(this.rangerServiceDefModel.get('resources'),function(obj){
				if(!_.isNull(obj)){
					var tmpObj =  that.model.get(obj.name);
					if(!_.isUndefined(tmpObj) && _.isObject(tmpObj)){
						var rPolicyResource = new RangerPolicyResource();
						rPolicyResource.set('values',tmpObj.resource.split(','));
						if(!_.isUndefined(tmpObj.isRecursive)){
							rPolicyResource.set('isRecursive', tmpObj.isRecursive)
						}
						if(!_.isUndefined(tmpObj.isExcludes)){
							rPolicyResource.set('isExcludes', tmpObj.isExcludes)
						}
						resources[obj.name] = rPolicyResource;
						that.model.unset(obj.name);
					}
				}
			});
			
			this.model.set('resources',resources);
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
		initializePathPlugins: function(options){
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

			this.fields[that.pathFieldName].editor.$el.find('[data-js="resource"]').tagit({
				autocomplete : {
					cache: false,
					source: function( request, response ) {
						var url = "service/plugins/services/lookupResource/"+that.rangerService.get('name');
						var context ={
							'userInput' : extractLast( request.term ),
							'resourceName' : that.pathFieldName,
							'resources' : {}
						};
						var val = that.fields[that.pathFieldName].editor.getValue();
						context.resources[that.pathFieldName] = _.isNull(val) || _.isEmpty(val) ? [] : val.resource.split(","); 
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
			        if(!_.isUndefined(options.regExpValidation) && !options.regExpValidation.regexp.test(ui.tagLabel)){
			        	that.fields[that.pathFieldName].$el.addClass('error');
			        	that.fields[that.pathFieldName].$el.find('.help-inline').html(options.regExpValidation.message);
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
			var type = options.containerCssClass, validRegExpString = true;
			if(!autocomplete)
				return{tags : true,width :'220px',multiple: true,minimumInputLength: 1, 'containerCssClass' : type};
			else {
				
				
				return {
					containerCssClass : options.type,
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
						term = _.escape(term);					
						if ($(data).filter(function() {
							return this.text.localeCompare(term) === 0;
						}).length === 0) {
							if(!_.isUndefined(options.regExpValidation) && !options.regExpValidation.regexp.test(term)){
									validRegExpString = false; 
							}else if($.inArray(term, this.val()) >= 0){
								return null;
							}else{
								return {
									id : term,
									text: term
								};
							}
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
							return that.getDataParams(term, options);
						},
						results: function (data, page) { 
							var results = [];
							if(!_.isUndefined(data)){
								if(_.isArray(data) && data.length > 0){
									results = data.map(function(m, i){	return {id : m, text: m};	});
								}
								if(!_.isUndefined(data.resultSize) &&  data.resultSize != "0"){
									results = data.vXStrings.map(function(m, i){	return {id : m.value, text: m.value};	});
								}
							}
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
						if(!validRegExpString && !_.isUndefined(options.regExpValidation)){
							return options.regExpValidation.message;
						}
						return "No Matches found";
					}
				};	
			}
		},
		getDataParams : function(term, options) {
			var resources = {},resourceName = options.type;
			var isParent = true, name = options.type, val = null,isCurrentSameLevelField = true;
			while(isParent){
				var currentResource = _.findWhere(this.rangerServiceDefModel.get('resources'), {'name': name });
				//same level type
				if(_.isUndefined(this.fields[currentResource.name])){
					var sameLevelName = 'sameLevel'+currentResource.level;
					name = this.fields[sameLevelName].editor.$resourceType.val()
					val = this.fields[sameLevelName].getValue();
					if(isCurrentSameLevelField){
						resourceName = name;
					}
				}else{
					val = this.fields[name].getValue();
				}
				resources[name] = _.isNull(val) ? [] : val.resource.split(','); 
				if(!_.isEmpty(currentResource.parent)){
					name = currentResource.parent;
				}else{
					isParent = false;
				}
				isCurrentSameLevelField = false;
			}
			var context ={
					'userInput' : term,
					'resourceName' : resourceName,
					'resources' : resources
				};
			return JSON.stringify(context);
		},
		formValidation : function(){
			var groupSet = false,permSet = false,groupPermSet = false;
			var userSet=false, userPerm = false, userPermSet =false,breakFlag =false;
			console.log('validation called..');
			this.formInputList.each(function(m){
				if(_.isEmpty(m.attributes)) return;
				if(m.has('groupName') || m.has('userName') || m.has('accesses') ){
					if(! breakFlag){
						groupSet = m.has('groupName') ? true : false;
						userSet = m.has('userName') ? true : false;
						permSet = m.has('accesses') ? true : false; 
						if(groupSet && permSet){
							groupPermSet = true;
							userPermSet = false;
						}else if(userSet && permSet){
							userPermSet = true;
							groupPermSet = false;
						}else{
							breakFlag=true;
						}
					}
				}
			});
			
			var auditStatus = this.fields.isAuditEnabled.editor.getValue();
			var obj = { groupPermSet	: groupPermSet , groupSet : groupSet,	
						userSet 		: userSet, isUsers:userPermSet,
						auditLoggin : auditStatus 
					};
			if(groupSet || userSet){
				obj['permSet'] = groupSet ? permSet : false;
				obj['userPerm'] = userSet ? permSet : false;
			}else{
				obj['permSet'] = permSet;
				obj['userPerm'] = userSet;
			}
			return obj;
		},
	});

	return RangerPolicyForm;
});
