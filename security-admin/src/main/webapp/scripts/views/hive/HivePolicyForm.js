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
    
	var VXPermMapList	= require('collections/VXPermMapList');
	var VXGroupList		= require('collections/VXGroupList');
	var VXAuditMapList	= require('collections/VXAuditMapList');
	var VXAuditMap		= require('models/VXAuditMap');
	var VXPermMap		= require('models/VXPermMap');
	var VXUserList		= require('collections/VXUserList');
	var FormInputItemList = require('views/common/FormInputItemList');
	var UserPermissionList = require('views/common/UserPermissionList');



	require('Backbone.BootstrapModal');
	require('backbone-forms.list');
	require('backbone-forms.templates');
	require('backbone-forms');

	var HivePolicyForm = Backbone.Form.extend(
	/** @lends PolicyForm */
	{
		_viewName : 'PolicyForm',
		
		type : {
			DATABASE : 1,
			TABLE    : 2,
			COLUMN   : 3,
			VIEW   : 4,
			UDF   : 5
		},

    	/**
		* intialize a new PolicyForm Form View 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a PolicyForm Form View");
    		Backbone.Form.prototype.initialize.call(this, options);

			_.extend(this, _.pick(options, 'assetModel'));
			this.initializeCollection();
			this.bindEvents();
		},
		
		/** all events binding here */
		bindEvents : function(){
			this.on('_vAuditListToggle:change', function(form, fieldEditor){
    			this.evAuditChange(form, fieldEditor);
    		});
			this.on('resourceType:change', function(form, fieldEditor){
    			this.evResourceTypeChange(form, fieldEditor);
    		});
			this.on('resourceStatus:change', function(form, fieldEditor){
    			this.evResourceStatusChange(form, fieldEditor);
    		});
			this.on('isTableInclude:change', function(form, fieldEditor){
    			this.isTableInclude(form, fieldEditor);
    		});
			this.on('isColumnInclude:change', function(form, fieldEditor){
    			this.isColumnInclude(form, fieldEditor);
    		});
		},
		initializeCollection: function(){
			this.permMapList = this.model.isNew() ? new VXPermMapList() : this.model.get('permMapList');
			this.auditList = this.model.isNew() ? new VXAuditMapList() : this.model.get('auditList');
			
		   
		   /*If the model passed to the fn is new return an empty collection
		    * otherwise return a collection that has models like 
		    * {
		    * 	groupId : 5,
		    * 	permissionList : [4,3]
		    * }
		    * The formInputList will be passed to the forminputitemlist view.
		    */
		   
		   this.formInputList = XAUtil.makeCollForGroupPermission(this.model);
		   this.userPermInputList = XAUtil.makeCollForUserPermission(this.model);
		   
		},
		/** fields for the form
		*/
	//	fields: ['name', 'description', '_vAuditListToggle', 'isEncrypt','isRecursive'],
		schema :function(){
			var that = this;
			//var plugginAttr = this.getPlugginAttr(true);
			
			return {
				policyName : {
					   type 		: 'Text',
					   title		: localization.tt("lbl.policyName") ,
//					   validators  	: [{'type' :'required'}]
					   editorAttrs 	:{ maxlength: 255}
				},
				databases : {
					type		: 'Select2Remote',
					title		: localization.tt("lbl.selectDatabaseName")+' *',
					editorAttrs :{'data-placeholder': 'Select Databases'},
					validators  : ['required'],//,{type:'regexp',regexp:/^[a-zA-Z*?][a-zA-Z0-9_'&-/\$]*[A-Za-z0-9]*$/i,message :localization.tt('validationMessages.enterValidName')}],
					pluginAttr  : this.getPlugginAttr(true,this.type.DATABASE),
	                options    : function(callback, editor){
	                    callback();
	                },
	                onFocusOpen : true

					
				},
				resourceType : {
					type		: 'Select',
					title		: 'Select Permission for [Tables|Views|UDF]',
				//	validators  : ['required'],
					editorAttrs :{disabled :'true','placeholder': 'Select Tables', 'class':'btn dropdown-toggle',
								'style': 'width: 89px;height: 29px;font-family: Tahoma;font-size: 14px;border-radius: 10px;border: 2px #cccccc solid;'},
					options : function(callback, editor){
						var permFor =['RESOURCE_TABLE','RESOURCE_UDF'];
						var temp = _.filter(XAEnums.ResourceType,function(val,key){
							if(_.contains(permFor,key))
									return true;
						});
						temp = _.sortBy(XAUtil.enumToSelectPairs(temp), function(n){ return !n.val; });
						callback(temp);
					}
					
				},
				tables : {
					type		: 'Select2Remote',
					title		: localization.tt("lbl.permForTable"),
					editorAttrs :{'data-placeholder': 'Select Tables', disabled :'disabled'},
			//		fieldAttrs :{'style' :'visibility:hidden'},
					//validators  : [{type:'regexp',regexp:/^[a-zA-Z*?][a-zA-Z0-9_'&-/\$]*[A-Za-z0-9]*$/i,message :localization.tt('validationMessages.enterValidName')}],
					pluginAttr  : this.getPlugginAttr(true,this.type.TABLE),
	                options    : function(callback, editor){
	                    callback();
	                }
				},
				isTableInclude : {
					type		: 'Switch',
					switchOn	: true,
					onText		: 'include',
					offText		: 'exclude',
					width		: 89, 
			    	height		: 22
					//fieldAttrs : {style : 'display:none;'}
				},
				udfs : {
					type		: 'Text',
					title		: localization.tt("lbl.permForUdf"),
					editorAttrs :{'placeholder': 'Enter UDF Name'}
			//		fieldAttrs :{'style' :'visibility:hidden'},
					//validators  : [{type:'regexp',regexp:/^[a-zA-Z*?][a-zA-Z0-9_'&-/\$]*[A-Za-z0-9]*$/i,message :localization.tt('validationMessages.enterValidName')}],
					/*pluginAttr  : this.getPlugginAttr(true,this.type.UDF),
	                options    : function(callback, editor){
	                    callback();
	                },*/
				},
				columns : {
					type		: 'Select2Remote',
					title		: localization.tt("lbl.selectColumnName"),
					//validators  : [{type:'regexp',regexp:/^[a-zA-Z*?][a-zA-Z0-9_'&-/\$]*[A-Za-z0-9]*$/i,message :localization.tt('validationMessages.enterValidName')}],
					editorAttrs :{'data-placeholder': 'Select Columns','disabled' :'disabled'},
					//fieldAttrs :{'disabled' :'disabled',},
					pluginAttr  : this.getPlugginAttr(true,this.type.COLUMN)
				},
				isColumnInclude : {
					type		: 'Switch',
					switchOn	: true,
					onText		: 'include',
					offText		: 'exclude',
					width		: 89, 
			    	height		: 22
			    	
					//fieldAttrs : {style : 'display:none;'}
				},
				_vAuditListToggle : {
					type		: 'Switch',
					title		: localization.tt("lbl.auditLogging"),
					listType	: 'VNameValue',
					switchOn	: true
				},
				isEncrypt : {
					type		: 'Switch',
					title		: localization.tt("lbl.encrypted"),
					switchOn	: false,
					fieldAttrs : {style : 'display:none;'}
				},
				permMapList : {
					getValue : function(){
						console.log(this);
					}
				},
				resourceStatus : {
					type		: 'Switch',
					title		: localization.tt("lbl.policyStatus"),
					onText		: 'enabled',
					offText		: 'disabled',
					width		: 89,
					height		: 22,
					switchOn	: true
				},
				description : { 
					type		: 'TextArea',
					title		: localization.tt("lbl.description"), 
				//	validators	: [/^[A-Za-z ,.'-]+$/i],
					template	:_.template('<div class="altField" data-editor></div>')
				}
			};	
		},

		evAuditChange : function(form, fieldEditor){
			XAUtil.checkDirtyFieldForToggle(fieldEditor);
			if(fieldEditor.getValue() == 1){
				this.model.set('auditList', new VXAuditMapList(new VXAuditMap({
					'auditType' : XAEnums.XAAuditType.XA_AUDIT_TYPE_ALL.value,//fieldEditor.getValue()//
					'resourceId' :this.model.get('id')
					
				})));
			} else {
					var validation  = this.formValidation();
					if(validation.groupPermSet || validation.isUsers)
						this.model.unset('auditList');
					else{
						XAUtil.alertPopup({
							msg :localization.tt("msg.policyNotHavingPerm"),
							callback : function(){
								fieldEditor.setValue('ON');
							}
						});
					}
			}
			console.log(fieldEditor);
		},
		evResourceStatusChange : function(form, fieldEditor){
			XAUtil.checkDirtyFieldForToggle(fieldEditor);
		},
		isTableInclude :function(form, fieldEditor){
			if(fieldEditor.getValue() == XAEnums.BooleanValue.BOOL_TRUE.value)
				form.fields.tables.editor.$el.siblings('.select2-container').find('ul').removeClass('textFieldWarning');
			else 
				form.fields.tables.editor.$el.siblings('.select2-container').find('ul').addClass('textFieldWarning');
		},
		isColumnInclude :function(form, fieldEditor){
			if(fieldEditor.getValue() == XAEnums.BooleanValue.BOOL_TRUE.value)
				form.fields.columns.editor.$el.siblings('.select2-container').find('ul').removeClass('textFieldWarning');
			else 
				form.fields.columns.editor.$el.siblings('.select2-container').find('ul').addClass('textFieldWarning');
		},
		evResourceTypeChange : function(form, fieldEditor){
			var that = this;
			form.$el.find('[data-editors="tables"]').hide();
			//form.$el.find('[data-fields="views"]').hide();
			form.$el.find('[data-editors="udfs"]').hide();
			form.$el.find('[data-fields="columns"]').hide();
			form.$el.find('[data-editors="isTableInclude"]').hide();
			form.$el.find('[data-editors="isColumnInclude"]').hide();
			this.fields.tables.editor.$el.select2('val','');
			//this.fields.views.editor.$el.select2('val','');
			this.fields.udfs.editor.$el.val('');
			this.fields.columns.editor.$el.select2('val','');
			this.showFields(fieldEditor);
			console.log(fieldEditor);
			XAUtil.checkDirtyFieldForToggle(fieldEditor);
		},
		showFields : function(fieldEditor){
			
			if(parseInt(fieldEditor.getValue()) == XAEnums.ResourceType.RESOURCE_TABLE.value){
				this.$el.find('[data-editors="tables"]').show();
				this.$el.find('[data-fields="columns"]').css('display','inline-block');
				this.$el.find('[data-editors="isTableInclude"]').css('display','inline-block');
				this.$el.find('[data-editors="isColumnInclude"]').css('display','inline-block');
				if(!this.fields.resourceType.editor.$el.is(':disabled'))
					this.fields.tables.editor.$el.select2('enable',true);
				this.fields.tables.editor.$el.val(this.fields.tables.getValue()).trigger('change');
				//this.fields.columns.editor.$el.val(this.fields.columns.getValue()).trigger('change');
			}
			if(parseInt(fieldEditor.getValue()) == XAEnums.ResourceType.RESOURCE_UDF.value){
				this.$el.find('[data-editors="tables"]').hide();
				this.$el.find('[data-fields="columns"]').hide();
				this.$el.find('[data-editors="isTableInclude"]').hide();
				this.$el.find('[data-editors="isColumnInclude"]').hide();
				this.$el.find('[data-editors="udfs"]').show();
				if(!this.fields.resourceType.editor.$el.is(':disabled'))
					this.fields.udfs.editor.$el.prop('disabled',false);
			}
		},
		/** on render callback */
		render: function(options) {
			 Backbone.Form.prototype.render.call(this, options);
			 

			this.initializePlugins();
			this.renderSelectTagsFields();
			this.renderCustomFields();
			if(!this.model.isNew())
				this.setUpForm();
			else{
				this.fields.resourceType.editor.$el.find('option:first').prop("selected",true);
			}
			if(this.model.isNew() && this.fields._vAuditListToggle.editor.getValue() == 1){
				this.model.set('auditList', new VXAuditMapList(new VXAuditMap({
					'auditType' : XAEnums.XAAuditType.XA_AUDIT_TYPE_ALL.value,//fieldEditor.getValue()//
					'resourceId' :this.model.get('id')
					
				})));
			}
		},
		setUpForm : function(){
			var that = this;
			this.setUpSwitches();
			if(!_.isEmpty(that.fields.resourceType.getValue()) || !_.isEmpty(this.fields.databases.editor.$el.val())){
				//that.fields.resourceType.editor.$el.val(that.fields.resourceType.getValue()).trigger('change');
					
				this.fields.resourceType.editor.$el.prop('disabled',false);
				this.showFields(that.fields.resourceType.editor);
			}
			if(!_.isEmpty(this.fields.databases.editor.$el.val()))
				this.fields.tables.editor.$el.prop('disabled',false);
			if((!_.isEmpty(this.fields.tables.editor.$el.val()))){
				this.fields.columns.editor.$el.removeAttr('disabled');
			}
			if((!_.isEmpty(this.fields.tables.editor.$el.val()))){
				this.fields.tables.editor.$el.siblings('.select2-container').find('ul').addClass('textFieldWarning');
			}
			if((!_.isEmpty(this.fields.columns.editor.$el.val()))){
				this.fields.columns.editor.$el.siblings('.select2-container').find('ul').addClass('textFieldWarning');
			}
		},
		setUpSwitches :function(){
			var that = this;
			var encryptStatus = false,auditStatus = false;
			auditStatus = this.model.has('auditList') ? true : false; 
			this.fields._vAuditListToggle.editor.setValue(auditStatus);
			
			_.each(_.toArray(XAEnums.BooleanValue),function(m){
				if(parseInt(that.model.get('isEncrypt')) == m.value)
					encryptStatus =  (m.label == XAEnums.BooleanValue.BOOL_TRUE.label) ? true : false;
			});
			this.fields.isEncrypt.editor.setValue(encryptStatus);
			
			if(parseInt(this.model.get('tableType')) == XAEnums.PolicyType.POLICY_EXCLUSION.value){
					this.fields.isTableInclude.editor.setValue(false);
			}
			if(parseInt(this.model.get('columnType')) == XAEnums.PolicyType.POLICY_EXCLUSION.value)
				this.fields.isColumnInclude.editor.setValue(false);
			
			if(parseInt(this.model.get('resourceStatus')) != XAEnums.BooleanValue.BOOL_TRUE.value)
				this.fields.resourceStatus.editor.setValue(false);
			
			
		},
		renderSelectTagsFields :function(){
			var that = this;
			this.fields.databases.editor.$el.on('change',function(e){
				console.log('change on database Name');
				that.checkMultiselectDirtyField(e, that.type.DATABASE);
				if(!_.isEmpty(e.currentTarget.value)){
					that.fields.resourceType.editor.$el.attr('disabled',false);
					that.fields.tables.editor.$el.attr('disabled',false);
					that.fields.tables.editor.$el.select2('enable',true);
					that.fields.udfs.editor.$el.prop('disabled',false);
					if((that.fields.tables.editor.$el.select2('val').length ) > 0 ) //|| that.fields.views.editor.$el.select2('val').length
						that.fields.columns.editor.$el.select2('enable',true);	
				}
				else{
					that.fields.resourceType.editor.$el.attr('disabled',true);
					that.fields.tables.editor.$el.select2('enable',false);
					that.fields.udfs.editor.$el.prop('disabled',true);
					that.fields.columns.editor.$el.select2('enable',false);
				}
					
			});
			this.fields.tables.editor.$el.on('change',function(e){
				console.log('change on table Name');
				that.checkMultiselectDirtyField(e, that.type.TABLE);
				if((!_.isEmpty(e.currentTarget.value))){
					that.fields.columns.editor.$el.select2('enable',true);
					
				}
				else{
					that.fields.columns.editor.$el.select2('enable',false);
				}
					
			});
			this.fields.columns.editor.$el.on('change',function(e){
				console.log('change on Column Name');
				that.checkMultiselectDirtyField(e, that.type.COLUMN);
			});
			
		},
		getDataParams : function(type, term){
			var dataParams = {
			//	dataSourceName : 'hadoopdev',
				dataSourceName : this.assetModel.get('name')
			// databaseName: term,
			};
			if (type == this.type.DATABASE && this.fields) {
				dataParams.databaseName = term;
			}
			if (type == this.type.TABLE && this.fields.databases) {
				dataParams.databaseName = this.fields.databases.editor.$el.select2('data')[0].text;
				dataParams.resourceType = this.fields.resourceType.getValue();
				dataParams.tableName = term;
			}
			if (type == this.type.COLUMN && this.fields.databases) {
				dataParams.databaseName = this.fields.databases.editor.$el.select2('data')[0].text;
				dataParams.resourceType = this.fields.resourceType.getValue();
				if(parseInt(this.fields.resourceType.getValue()) == XAEnums.ResourceType.RESOURCE_TABLE.value && this.fields.tables)
					dataParams.tableName = this.fields.tables.editor.$el.select2('data')[0].text;
				dataParams.columnName = term;
			}
			return dataParams;
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
							if(data.resultSize != "0"){
								results = data.vXStrings.map(function(m, i){	return {id : m.value, text: m.value};	});
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
					userSet = m.has('userId') || m.has('userName') ? true : false ;//|| userSet; 
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
			auditStatus = this.fields._vAuditListToggle.editor.getValue();
			encryptStatus = this.fields.isEncrypt.editor.getValue();
			var auditLoggin = (auditStatus == XAEnums.BooleanValue.BOOL_TRUE.value) ? true : false;
			var encrypted = (encryptStatus == XAEnums.BooleanValue.BOOL_TRUE.value) ? true : false;
			
			return {groupPermSet: groupPermSet , groupSet : groupSet,permSet : permSet,auditLoggin :auditLoggin,encrypted : encrypted,
				userSet : userSet,userPerm:userPerm,isUsers:isUsers};	
		},
		/** all custom field rendering */
		renderCustomFields: function(){
			var that = this;
			this.groupList = new VXGroupList();
			var params = {sortBy : 'name'};
			this.groupList.setPageSize(100,{fetch:false});
			this.groupList.fetch({
					cache :true,
					data : params
				}).done(function(){
					that.$('[data-customfields="groupPerms"]').html(new FormInputItemList({
						collection 	: that.formInputList,
						groupList  	: that.groupList,
						model 		: that.model,
						policyType 	: XAEnums.AssetType.ASSET_HIVE.value
					
					}).render().el);
			});
			
			this.userList = new VXUserList();
			var params = {sortBy : 'name'};
			this.userList.setPageSize(100,{fetch:false});
			this.userList.fetch({
				cache :true,
				data : params
				}).done(function(){
					that.$('[data-customfields="userPerms"]').html(new UserPermissionList({
						collection 	: that.userPermInputList,
						model 		: that.model,
						userList 	: that.userList,
						policyType 	: XAEnums.AssetType.ASSET_HIVE.value
					}).render().el);
			});
		},
		afterCommit : function(){
			var that = this;
			//TODO FIXME remove the hard coding 
			//this.model.set('assetId', XAGlobals.hardcoded.HiveAssetId);
			this.model.set('assetId', this.assetModel.id);
			var permMapList = new VXPermMapList();
			this.formInputList.each(function(m){
				if(!_.isUndefined(m.get('groupId'))){
					var uniqueID = _.uniqueId(new Date()+':');
					_.each(m.get('groupId').split(","),function(groupId){
						_.each(m.get('_vPermList'), function(perm){
							var params = {
									//groupId: m.get('groupId'),
									groupId: groupId,
									permFor : XAEnums.XAPermForType.XA_PERM_FOR_GROUP.value,
									permType : perm.permType,
									permGroup : uniqueID
							};
							//TODO FIXME remove the hardcoding
							if(parseInt(groupId) == perm.groupId)
								params = $.extend(params, {id : perm.id});
							permMapList.add(new VXPermMap(params));
						}, this);
					});
				}
			}, this);
			
			this.userPermInputList.each(function(m){
				if(!_.isUndefined(m.get('userId'))){
					var uniqueID = _.uniqueId(new Date()+':');
					_.each(m.get('userId').split(","),function(userId){
						_.each(m.get('_vPermList'), function(perm){
							var params = {
									//groupId: m.get('groupId'),
									userId: userId,
									permFor : XAEnums.XAPermForType.XA_PERM_FOR_USER.value,
									permType : perm.permType,
									permGroup : uniqueID
							};
							if(parseInt(userId) == perm.userId)
								params = $.extend(params, {id : perm.id});
							//TODO FIXME remove the hardcoding
							permMapList.add(new VXPermMap(params));
						}, this);
					});
				}
			}, this);
			
			this.model.set('permMapList', permMapList);
			
			var resourceType = this.model.get('resourceType');
			var resourceTypeTable = XAEnums.ResourceType.RESOURCE_TABLE.value == resourceType ? true :false;
			var resourceTypeUdf = XAEnums.ResourceType.RESOURCE_UDF.value == resourceType ? true :false;
			
			var columns = _.isEmpty(this.model.get('columns')) ? true : false;
			var perm1 = resourceTypeTable && _.isEmpty(this.model.get('tables')) && columns;
			var perm3 = resourceTypeUdf   && _.isEmpty(this.model.get('udfs')) ;
			
			if(_.isEmpty(this.model.get('resourceType')))
				this.model.set('resourceType',XAEnums.ResourceType.RESOURCE_DB.value);
			else{
				
				if(perm1 || perm3){ //if(perm1 || perm2 || perm3){
					this.model.set('resourceType',XAEnums.ResourceType.RESOURCE_DB.value);
					this.model.unset('columns');
					this.model.unset('udfs');
				}else{
					if(columns){
						if(resourceTypeTable)
							this.model.set('resourceType',XAEnums.ResourceType.RESOURCE_TABLE.value);
						else
							this.model.set('resourceType',XAEnums.ResourceType.RESOURCE_UDF.value);
					}else{
						
						if(resourceTypeTable && !_.isEmpty(this.model.get('tables')))
							this.model.set('resourceType',XAEnums.ResourceType.RESOURCE_COLUMN.value);
						else{
							this.model.unset('columns');							
							this.model.set('resourceType',XAEnums.ResourceType.RESOURCE_DB.value);
						}
					}
				}
			}
			
			if(this.fields.resourceType.getValue() != XAEnums.ResourceType.RESOURCE_UDF.value){
				if(!_.isEmpty(this.model.get('tables'))){
					if(this.fields.isTableInclude.editor.getValue() == XAEnums.BooleanValue.BOOL_TRUE.value)
						this.model.set('tableType',XAEnums.PolicyType.POLICY_INCLUSION.value);
					else
						this.model.set('tableType',XAEnums.PolicyType.POLICY_EXCLUSION.value);
				}
				if(!_.isEmpty(this.model.get('columns'))){
					if(this.fields.isColumnInclude.editor.getValue() == XAEnums.BooleanValue.BOOL_TRUE.value)
						this.model.set('columnType',XAEnums.PolicyType.POLICY_INCLUSION.value);
					else
						this.model.set('columnType',XAEnums.PolicyType.POLICY_EXCLUSION.value);
				}
			}
			this.model.unset('isTableInclude');
			this.model.unset('isColumnInclude');
			
			if(this.model.get('resourceStatus') != XAEnums.BooleanValue.BOOL_TRUE.value){
				this.model.set('resourceStatus', XAEnums.ActiveStatus.STATUS_DISABLED.value);
			}
		},
		checkMultiselectDirtyField : function(e, type){
			var elem = $(e.currentTarget),columnName='',nameList = [], newNameList = [];
			switch(type){
				case 1 :columnName = 'databases';break;
				case 2 :columnName = 'tables';break;
				case 3 :columnName = 'columns';break;
			}
			if(!_.isUndefined(this.model.get(columnName)) && !_.isEqual(this.model.get(columnName),""))
				nameList = this.model.get(columnName).split(',');
			if(!_.isEqual(e.currentTarget.value, ""))
				newNameList = e.currentTarget.value.split(',');
			XAUtil.checkDirtyField(nameList, newNameList, elem);
		},
		/** all post render plugin initialization */
		initializePlugins: function(){
		}

	});

	return HivePolicyForm;
});
