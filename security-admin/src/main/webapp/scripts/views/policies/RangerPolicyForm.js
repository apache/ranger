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

		templateData : function() {
			
			return this.getTemplateData();
		},
    	/**
		* intialize a new RangerPolicyForm Form View 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a RangerPolicyForm Form View");
			_.extend(this, _.pick(options, 'rangerServiceDefModel', 'rangerService'));
			this.setupForm()
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
			this.on('isRecursive:change', function(form, fieldEditor){
    			this.evRecursiveChange(form, fieldEditor);
    		});
			this.on('resourceStatus:change', function(form, fieldEditor){
    			this.evResourceStatusChange(form, fieldEditor);
    		});
			
			/*this.on('sameLevelType:change', function(form, fieldEditor){
				this.evResourceTypeChange(form, fieldEditor);
			});*/
		},

		/** fields for the form
		*/
		fields: ['name', 'description', 'isEnabled', 'isAuditEnabled'],
		schema :function(){
			return this.getSchema();
		},
		getSchema : function(){
			var attrs = {};
			var schemaNames = this.rangerServiceDefModel.get('name') == "hdfs" ? ['description', 'isRecursive', 'isAuditEnabled'] : ['description', 'isAuditEnabled'];
			
			var formDataType = new BackboneFormDataType();
			attrs = formDataType.getFormElements(this.rangerServiceDefModel.get('resources'),this.rangerServiceDefModel.get('enums'), attrs, this);
			
			attrs = this.setSameLevelField(attrs, schemaNames);
			
			var attr1 = _.pick(_.result(this.model,'schemaBase'), 'name','isEnabled');
			var attr2 = _.pick(_.result(this.model,'schemaBase'),schemaNames);
			return _.extend(attr1,_.extend(attrs,attr2));
		},
		setSameLevelField : function(attrs, schemaNames) {
			var level = [],sameLevel = [],that = this;
			this.sameLevelType = [],this.selectOptions = {};
			this.sameLevelFound = false;
			//Get array of all levels like [1,2,2,3,4,4,4,5,6]
			_.each(attrs, function(obj){ level.push(obj.level) })
			
			/*	count levels 
				counts = { 1 : 1, 2 : 2, 3 : 1, 4: 3, 5 : 1, 6 : 1}
			*/
			var counts = {};
			level.forEach(function(x) { counts[x] = (counts[x] || 0)+1; });
			//create level counter array which has more than one same level
			_.each(counts, function(cnt,l) {
				if(cnt > 1){
					sameLevel.push(l);
					this.sameLevelFound = true;
				}
			}, this);
			
			if(this.sameLevelFound){
				this.schemaBase = ['name','isEnabled'];
				this.schemaBase1 = schemaNames;
				var editorsAttr = [], fieldAttrs = [];
				//iterate over same level array
				_.each(sameLevel, function(lev, i) {
					//get same level resources
					var OptionsAttrs = _.filter(attrs,function(field){ if(field.level == lev) return field;})
					var optionsTitle = _.map(OptionsAttrs,function(field){ return field.name;});
					
					//cretae selectType for same level resource
					attrs['sameLevelType'+lev]  = {
							type 	: 'Select',
							options	:  optionsTitle,
							editorAttrs : {'class':'btn dropdown-toggle','style': 'width: 100px;height: 29px;font-family: Tahoma;font-size: 14px;border-radius: 10px;border: 2px #cccccc solid;'}
					};
					//hide all select options
					_.each(optionsTitle, function(field,i){ 
						if( i > 0 ) attrs[field].editorAttrs['style']='display:none';
					})
					//create sameLevelType array
					var tmp = { 'name' : "sameLevelType"+lev, 'options' : optionsTitle.toString() };
					this.sameLevelType.push(tmp);
				
					editorsAttr = editorsAttr.concat(optionsTitle)
				}, this)
				
				//create fieldAttrs array 
				_.each(attrs, function(obj){
					if(!_.isUndefined(obj.name) && $.inArray(obj.name, editorsAttr) < 0 ){
							fieldAttrs.push(obj.name);
					} 
				});
				
				// Add Resources in same order as give in JSON
				var addToschemaBase = true;
				_.each(attrs,function(field, i) {
					if(!_.isUndefined(field.name)){
						if($.inArray(field.name, editorsAttr) < 0 && addToschemaBase){
							this.schemaBase.push(field.name)
						}
						if($.inArray(field.name, editorsAttr) >= 0){
							addToschemaBase = false;
						}
						if($.inArray(field.name, editorsAttr) < 0 && !addToschemaBase){
							this.schemaBase1.unshift(field.name)
						}
						
					}
				}, this);
				
				
			}
			//add change events on all sameLevelType
			_.each(this.sameLevelType, function(obj, i){
				that.on(obj.name+':change', function(form, fieldEditor) {
					this.evResourceTypeChange(form, fieldEditor);
				});
			});
			return attrs;
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
			this.setupSameLevelType();
		},
		evAuditChange : function(form, fieldEditor){
			XAUtil.checkDirtyFieldForToggle(fieldEditor);
		},
		evRecursiveChange : function(form, fieldEditor){
			XAUtil.checkDirtyFieldForToggle(fieldEditor);
		},
		evResourceStatusChange : function(form, fieldEditor){
			XAUtil.checkDirtyFieldForToggle(fieldEditor);
		},
		getTemplateData : function() {
			var obj={ 'fieldsets' : true };
			if(this.sameLevelFound){ 
				obj  = { sameLevelType  : this.sameLevelType, 
						 schemaBase	  	: this.schemaBase.toString(),
						 schemaBase1	: this.schemaBase1.toString(),
						 
					};
				if(this.schemaBase.length <= 2)
					obj.marginBottom57 = 'margin-bottom-57';
			}
			return obj;
		},
		setupForm : function() {
			_.each(this.model.attributes.resources,function(obj,key){
				this.model.set(key, obj.values.toString());
				if(!_.isUndefined(obj.isRecursive)){
					this.model.set('isRecursive', obj.isRecursive);
				}
			},this)
		},
		setUpSwitches :function(){
			var that = this;
			this.fields.isAuditEnabled.editor.setValue(this.model.get('isAuditEnabled'));
			this.fields.isEnabled.editor.setValue(this.model.get('isEnabled'));
			if(!_.isUndefined(this.fields.isRecursive))
				this.fields.isRecursive.editor.setValue(this.model.get('isRecursive'));
		},
		setupSameLevelType : function() {
			//setup sameLevelType `select` if there
			_.each(this.sameLevelType, function(obj, i){
				if(!this.model.isNew()){
					var sameLevelOpt = obj.options.split(',');
					var sameLevelVal = _.find(sameLevelOpt, function(type){ if(!_.isEmpty(this.model.get(type))) return type;},this)
					console.log(this.model.attributes)
					this.model.set(obj.name,sameLevelVal);
					this.fields[obj.name].editor.$el.val(sameLevelVal).trigger('change')
				}else{
					this.fields[obj.name].editor.$el.trigger('change')
				}
			}, this);
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
					rPolicyResource.set('values',that.model.get(obj.name).split(','));
					rPolicyResource.set('isRecursive',that.model.get('isRecursive'))
					resources[obj.name] = rPolicyResource;
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
			var that= this;	
			function split( val ) {
				return val.split( /,\s*/ );
			}
			function extractLast( term ) {
				return split( term ).pop();
			}

			this.fields[that.pathFieldName].editor.$el.bind( "keydown", function( event ) {
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
						var p = $.getJSON( "service/assets/hdfs/resources", {
							dataSourceName: that.rangerService.get('name'),
							baseDirectory: extractLast( request.term )
						}).done(function(data){
							if(data.vXStrings){
								response(data.vXStrings);
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
			});
	
			
		},
		getPlugginAttr :function(autocomplete, options){
			var that =this;
			var type = options.containerCssClass;
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
						dataType: 'json',
						params : {
							timeout: 3000
						},
						cache: false,
						data: function (term, page) {
							return _.extend(that.getDataParams(type, term));
							
						},
						results: function (data, page) { 
							var results = [];
							if(!_.isUndefined(data)){
								if(data.resultSize != "0"){
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
