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
	var GroupPermList 	= require('views/policies/GroupPermList');
	var UserPermList 	= require('views/policies/UserPermList');
	var RangerPolicyResource		= require('models/RangerPolicyResource');
	var RangerPolicyResourceList	= require('collections/RangerPolicyResourceList');

	require('backbone-forms.list');
	require('backbone-forms.templates');
	require('backbone-forms');
	require('backbone-forms.XAOverrides');
	require('jquery-ui');
	require('tag-it');

	var PolicyForm = Backbone.Form.extend(
	/** @lends PolicyForm */
	{
		_viewName : 'PolicyForm',

    	/**
		* intialize a new PolicyForm Form View 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a PolicyForm Form View");
			_.extend(this, _.pick(options, 'rangerServiceDefModel', 'rangerService'));
			this.setupForm()
    		Backbone.Form.prototype.initialize.call(this, options);

			this.initializeCollection();
			this.bindEvents();
		},
		type : {
			DATABASE : 1,
			TABLE    : 2,
			COLUMN   : 3,
			VIEW   : 4,
			UDF   : 5
		},
		initializeCollection: function(){
			this.permMapList = this.model.isNew() ? new VXPermMapList() : this.model.get('permMapList');
			this.auditList = this.model.isNew() ? new VXAuditMapList() : this.model.get('auditList');
			
			//this.userList.fetch();
			

			/*If the model passed to the fn is new return an empty collection
			 * otherwise return a collection that has models like 
			 * {
			 * 	groupId : 5,
			 * 	permissionList : [4,3]
			 * }
			 * The formInputList will be passed to the forminputitemlist view.
			 */

			this.formInputList 		= XAUtil.makeCollForGroupPermission(this.model);
			this.userPermInputList  = XAUtil.makeCollForUserPermission(this.model);

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
		},

		/** fields for the form
		*/
		fields: ['name', 'description', 'isEnabled', 'isAuditEnabled'],
		schema :function(){
			var attrs = {};
			var that = this;
			_.each(this.rangerServiceDefModel.get('resources'),function(v,k){ 
				if (v != null) {

					var formObj = {};
					switch(v.type){
							case 'string' : formObj.type = 'Text'; break;
							case 'select2' : 
								formObj.type = 'Select2Remote';
								formObj.pluginAttr = that.getPlugginAttr(false),
								formObj.editorAttrs = {'data-placeholder': v.label },
								formObj.options = function(callback, editor){
				                    callback();
				                },
				                formObj.onFocusOpen = true
								break;
							case 'path' : 
								formObj.type = 'Text';
								that.initilializePathPlugin = true;
								that.pathFieldName = v.name;
								break;
							default : formObj.type = 'Text'; break;
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
			var attr1 = _.pick(_.result(this.model,'schemaBase'), 'name','isEnabled');
			var attr2 = _.pick(_.result(this.model,'schemaBase'),'description', 'isRecursive', 'isAuditEnabled');
			return _.extend(attr1,_.extend(attrs,attr2));
		},
		/** on render callback */
		render: function(options) {
			var that = this;
			
			Backbone.Form.prototype.render.call(this, options);

			if(!_.isUndefined(this.initilializePathPlugin) && this.initilializePathPlugin){ 
				this.initializePathPlugins();
			}
			this.renderCustomFields();
		/*	if(!this.model.isNew()){
				this.setUpSwitches();
			}
			if(this.model.isNew() && this.fields._vAuditListToggle.editor.getValue() == 1){
				this.model.set('auditList', new VXAuditMapList(new VXAuditMap({
					'auditType' : XAEnums.XAAuditType.XA_AUDIT_TYPE_ALL.value,//fieldEditor.getValue()//
					'resourceId' :this.model.get('id')
					
				})));
			}*/
			this.$el.find('.field-isEnabled').find('.control-label').remove();
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
		formValidation : function(){
			var groupSet = false,permSet = false,auditStatus= false,encryptStatus= false,groupPermSet = false,
							userSet=false,userPerm = false,isUsers =false;
			console.log('validation called..');
			var breakFlag =false;
			this.formInputList.each(function(m){
				if(m.has('groupId') ||  m.has('_vPermList')){
					if(! breakFlag){
						groupSet = m.has('groupId') ? true : false ; 
						if(!m.has('_vPermList')){
							permSet = false;
						}else
							permSet = true;
						if(groupSet && permSet)
							groupPermSet = true;
						else
							breakFlag=true;
					}
				}
			});
			breakFlag = false;
			
			this.userPermInputList.each(function(m){
					if(! breakFlag){
						userSet = m.has('userId') || m.has('userName') ? true : false ; 
						if(!m.has('_vPermList')){
							userPerm = false;
						}else
							userPerm = true;
						if(userSet && userPerm)
							isUsers = true;
						else
							breakFlag=true;
					}
			});
			var auditLoggin = this.fields.isAuditEnabled.editor.getValue();

			return {groupPermSet: groupPermSet , groupSet : groupSet,permSet : permSet,auditLoggin :auditLoggin,
				userSet : userSet,userPerm:userPerm,isUsers:isUsers};
		},
		setupForm : function() {
			var resourcePath = _.map(this.model.get('resources'), function(obj,i){
				if( i == 0 ) this.model.set('isRecursive', obj.isRecursive) 
				return obj.value; 
			}, this);
			if(this.rangerService.get('type').toUpperCase() == XAEnums.ServiceType.Service_HDFS.label.toUpperCase()){
				this.model.set('path', resourcePath.toString());
			}else if(this.rangerService.get('type').toUpperCase() == XAEnums.ServiceType.Service_HIVE.label.toUpperCase()){
				_.each(resourcePath, function(path) {
					var temp  = path.split("/");
					if(!_.isUndefined(temp[1]))	this.model.set('database', temp[1])	
					if(!_.isUndefined(temp[2]))	this.model.set('table', temp[2])
					if(!_.isUndefined(temp[3]))	this.model.set('column', temp[3])
					
				},this);
			}else if(this.rangerService.get('type').toUpperCase() == XAEnums.ServiceType.Service_HBASE.label.toUpperCase()){
				_.each(resourcePath, function(path) {
					var temp  = path.split("/");
					if(!_.isUndefined(temp[1]))	this.model.set('table', temp[1])	
					if(!_.isUndefined(temp[2]))	this.model.set('column-family', temp[2])
					if(!_.isUndefined(temp[3]))	this.model.set('column', temp[3])
					
				},this);
			}else if(this.rangerService.get('type').toUpperCase() == XAEnums.ServiceType.Service_KNOX.label.toUpperCase()){
				_.each(resourcePath, function(path) {
					var temp  = path.split("/");
					if(!_.isUndefined(temp[1]))	this.model.set('topology', temp[1])	
					if(!_.isUndefined(temp[2]))	this.model.set('service', temp[2])
					
				},this);
			}else if(this.rangerService.get('type').toUpperCase() == XAEnums.ServiceType.Service_STORM.label.toUpperCase()){
				var resourcePath = _.map(this.model.get('resources'), function(obj,i){
					if( i == 0 ) this.model.set('isRecursive', obj.isRecursive) 
					return obj.value; 
				}, this);
				_.each(resourcePath, function(path) {
					var temp  = path.split("/");
					if(!_.isUndefined(temp[1]))	this.model.set('topology', temp[1])	
				},this);
			}
		},
		setUpSwitches :function(){
			var that = this;
			var encryptStatus = false,auditStatus = false,recursiveStatus = false;
			auditStatus = this.model.has('auditList') ? true : false; 
			this.fields._vAuditListToggle.editor.setValue(auditStatus);
			
			_.each(_.toArray(XAEnums.BooleanValue),function(m){
				if(parseInt(that.model.get('isEncrypt')) == m.value)
					encryptStatus =  (m.label == XAEnums.BooleanValue.BOOL_TRUE.label) ? true : false;
				if(parseInt(that.model.get('isRecursive')) == m.value)
					recursiveStatus =  (m.label == XAEnums.BooleanValue.BOOL_TRUE.label) ? true : false;
			});
			this.fields.isEncrypt.editor.setValue(encryptStatus);
			this.fields.isRecursive.editor.setValue(recursiveStatus);
			if(parseInt(this.model.get('resourceStatus')) != XAEnums.BooleanValue.BOOL_TRUE.value)
				this.fields.resourceStatus.editor.setValue(false);
		},
		/** all custom field rendering */
		renderCustomFields: function(){
			var that = this;
			var accessType = this.rangerServiceDefModel.get('accessTypes').filter(function(val) { return val !== null; });
			var policyType = this.rangerService.get('type')
			_.each(XAEnums.ServiceType, function(obj){ 
				if(that.rangerService.get('type').toUpperCase() == obj.label.toUpperCase())
					policyType = obj.value;
			});
			this.groupList = new VXGroupList();
			var params = {sortBy : 'name'};
			this.groupList.setPageSize(100,{fetch:false});
			this.groupList.fetch({
					cache :true,
					data : params
				}).done(function(){
					that.$('[data-customfields="groupPerms"]').html(new GroupPermList({
						collection : that.formInputList,
						groupList  : that.groupList,
						model : that.model,
						policyType 	: policyType,
						accessTypes : accessType,
						rangerServiceDefModel : that.rangerServiceDefModel
					}).render().el);
			});
			
			this.userList = new VXUserList();
			var params = {sortBy : 'name'};
			this.userList.setPageSize(100,{fetch:false});
			this.userList.fetch({
					cache :true,
					data: params
				}).done(function(){
					that.$('[data-customfields="userPerms"]').html(new UserPermList({
						collection : that.userPermInputList,
						model : that.model,
						userList : that.userList,
						policyType 	: policyType,
						accessTypes : accessType,
						rangerServiceDefModel : that.rangerServiceDefModel
					}).render().el);
			});
		},
	
		beforeSave : function(){
			var that = this, resources = '';
			this.model.set('service',this.rangerService.get('name'));
			if(that.rangerService.get('type').toUpperCase() == XAEnums.ServiceType.Service_HDFS.label.toUpperCase())
				resources = this.model.get('path').split(',');
			else if(that.rangerService.get('type').toUpperCase() == XAEnums.ServiceType.Service_HIVE.label.toUpperCase()){
				resources = "/"+this.model.get('database');
				resources += !_.isEmpty(this.model.get('table')) ? "/"+this.model.get('table') : '';
				resources += !_.isEmpty(this.model.get('column')) ? "/"+this.model.get('column') : '';
				resources = resources.split(',');
			}else if(that.rangerService.get('type').toUpperCase() == XAEnums.ServiceType.Service_HBASE.label.toUpperCase()){
				resources = "/"+this.model.get('table');
				resources += !_.isEmpty(this.model.get('column-family')) ? "/"+this.model.get('column-family') : '';
				resources += !_.isEmpty(this.model.get('column')) ? "/"+this.model.get('column') : '';
				resources = resources.split(',');
			}else if(that.rangerService.get('type').toUpperCase() == XAEnums.ServiceType.Service_KNOX.label.toUpperCase()){
				resources = "/"+this.model.get('topology')+"/"+this.model.get('service');
				resources = resources.split(',');
			}else if(that.rangerService.get('type').toUpperCase() == XAEnums.ServiceType.Service_STORM.label.toUpperCase()){
				resources = "/"+this.model.get('topology')
				resources = resources.split(',');
			}
			var rPolicyResourceList = new RangerPolicyResourceList();
			_.each(resources, function(val){
				var rPolicyResource = new RangerPolicyResource();
				rPolicyResource.set('type',that.rangerService.get('type'));
				rPolicyResource.set('value',val);
				rPolicyResource.set('isRecursive',that.model.get('isRecursive'))
				rPolicyResource.set('isExcludes',null)
				rPolicyResourceList.add(rPolicyResource);
			});
			
			
			
			
//			this.model.set('isEnabled',this.model.get('isEnabled')+"");
//			this.model.set('isAuditEnabled',this.model.get('isAuditEnabled')+"");
			this.model.set('resources',rPolicyResourceList);
			this.model.unset('isRecursive');
			this.model.unset('path');
			
			//Set UserGroups Permission
			
			var RangerPolicyItem = Backbone.Collection.extend();
			var policyItemList = new RangerPolicyItem();
			this.formInputList.each(function(m){
				if(!_.isUndefined(m.get('groupName'))){
					var RangerPolicyItem=Backbone.Model.extend()
					var policyItem = new RangerPolicyItem();
					policyItem.set('groups',m.get('groupName').split(','))
					
					var RangerPolicyItemAccessList = Backbone.Collection.extend();
					var rangerPlcItemAccessList = new RangerPolicyItemAccessList(m.get('accesses'));
					policyItem.set('accesses', rangerPlcItemAccessList)
					policyItemList.add(policyItem)
					
				}
			}, this);
			this.userPermInputList.each(function(m){
				if(!_.isUndefined(m.get('userName'))){
					var RangerPolicyItem=Backbone.Model.extend()
					var policyItem = new RangerPolicyItem();
					policyItem.set('users',m.get('userName').split(','))
					
					var RangerPolicyItemAccessList = Backbone.Collection.extend();
					var rangerPlcItemAccessList = new RangerPolicyItemAccessList(m.get('accesses'));
					policyItem.set('accesses', rangerPlcItemAccessList)
					policyItemList.add(policyItem)
					
				}
			}, this);
			this.model.set('policyItems', policyItemList)
			
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
//			        this.value = tags;
			        /*if(_.contains(ui.tagLabel,','))
			        	tags = ui.tagLabel.split(',');
			        	this.value = tags;*/
					}
			});
			/*this.fields.name.editor.$el.tagit({
				beforeTagAdded: function(event, ui) {
		        // do something special
				var tags =  [];
		        console.log(ui.tag);
		        if(_.contains(ui.tagLabel,','))
		        	tags = ui.tagLabel.split(',');
		        	this.value = tags;
				}
			});*/
			
		},
		getPlugginAttr :function(autocomplete, searchType){
			var that =this;
			var type = searchType;
			if(!autocomplete)
				return{tags : true,width :'220px',multiple: true,minimumInputLength: 1};
			else {
				
				
				return {
					closeOnSelect : true,
					//placeholder : 'Select User',
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
					/*query: function (query) {
						var url = "service/assets/hive/resources";
						var data = _.extend(that.getDataParams(type, query.term));
						//var results = [ {id: query.term, path: query.term}];

						$.get(url, data, function (resp) {
							var serverRes = [];
							if(resp.resultSize){
								serverRes = resp.vXStrings.map(function(m, i){	return {id : m.text, path: m.text};	});
							}
							query.callback({results: serverRes});
						}, 'json');

						//query.callback({results: results});
					},*/

					ajax: { 
						url: "service/assets/hive/resources",
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
							/*$.ajax.error(function(data) { 
								console.log("ajax failed");
								return {
									results : []
								};
							});*/

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
	});

	return PolicyForm;
});
