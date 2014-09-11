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
	var FormInputItemList = require('views/common/FormInputItemList');
	var UserPermissionList = require('views/common/UserPermissionList');

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
    		Backbone.Form.prototype.initialize.call(this, options);

			_.extend(this, _.pick(options,'assetModel'));
			this.initializeCollection();
			this.bindEvents();
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
			this.on('_vAuditListToggle:change', function(form, fieldEditor){
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
		fields: ['name', 'description', '_vAuditListToggle', 'isEncrypt','isRecursive'],
		schema :{
			policyName : {
				   type 		: 'Text',
				   title		: localization.tt("lbl.policyName"),
//				   validators  	: [{'type' :'required'}]
				   editorAttrs 	:{ maxlength: 255},
			},
			name : {
			   type 		: 'Text',
			   title		: localization.tt("lbl.resourcePath") +' *',
			   validators  	: [{'type' :'required'},
				   function checkPath(resourcePaths, formValues) {
					   var errorPath=[];
					   _.each(resourcePaths.split(','),function(path){
						   if(!(/^(\/(|[a-zA-Z0-9*?_/\.\-\\])*)$/i).test(path)){
							   if(!_.isEmpty(path)){
								   errorPath.push(path);
							   }
							   return;
						   }
					   });
					   if(!_.isEmpty(errorPath))
						   return {
							   type: 'name',
							   message: 'Please enter valid resource path : '+errorPath.join(',')
						   };
				   }
				] 
			},
			isRecursive : {
				type		: 'Switch',
				title		: localization.tt('lbl.includesAllPathsRecursively'),
				switchOn	: false,
				onText 		: 'YES',
				offText		: 'NO'
			},
			description : { 
				type		: 'TextArea',
				title		: localization.tt("lbl.description"), 
			//	validators	: [/^[A-Za-z ,.'-]+$/i],
				template	:_.template('<div class="altField" data-editor></div>')
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
			isEnabled : {
				type		: 'Switch',
				title		: localization.tt("lbl.policyStatus"),
				onText		: 'enabled',
				offText		: 'disabled',
				width		: '80',
				switchOn	: true
			},
			permMapList : {
				getValue : function(){
					console.log(this);
				}
			},
			userIdList : {
				getValue : function(){
					console.log("userIdList : "+this);
				}
			},
			resourceStatus : {
				type		: 'Switch',
				title		: localization.tt("lbl.policyStatus"),
				onText		: 'enabled',
				offText		: 'disabled',
				width		: '85',
				switchOn	: true
			}
		},
		/** on render callback */
		render: function(options) {
			var that = this;
			 Backbone.Form.prototype.render.call(this, options);

			this.initializePlugins();
			this.renderCustomFields();
			if(!this.model.isNew()){
				this.setUpSwitches();
			}
			if(this.model.isNew() && this.fields._vAuditListToggle.editor.getValue() == 1){
				this.model.set('auditList', new VXAuditMapList(new VXAuditMap({
					'auditType' : XAEnums.XAAuditType.XA_AUDIT_TYPE_ALL.value,//fieldEditor.getValue()//
					'resourceId' :this.model.get('id')
					
				})));
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
			auditStatus = this.fields._vAuditListToggle.editor.getValue();
			encryptStatus = this.fields.isEncrypt.editor.getValue();
			var auditLoggin = (auditStatus == XAEnums.BooleanValue.BOOL_TRUE.value) ? true : false;
			var encrypted = (encryptStatus == XAEnums.BooleanValue.BOOL_TRUE.value) ? true : false;

			return {groupPermSet: groupPermSet , groupSet : groupSet,permSet : permSet,auditLoggin :auditLoggin,encrypted : encrypted,
				userSet : userSet,userPerm:userPerm,isUsers:isUsers};
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
			this.groupList = new VXGroupList();
			var params = {sortBy : 'name'};
			this.groupList.setPageSize(100,{fetch:false});
			this.groupList.fetch({
					cache :true,
					data : params
				}).done(function(){
					that.$('[data-customfields="groupPerms"]').html(new FormInputItemList({
						collection : that.formInputList,
						groupList  : that.groupList,
						model : that.model,
						policyType 	: XAEnums.AssetType.ASSET_HDFS.value
					}).render().el);
			});
			
			this.userList = new VXUserList();
			var params = {sortBy : 'name'};
			this.userList.setPageSize(100,{fetch:false});
			this.userList.fetch({
					cache :true,
					data: params
				}).done(function(){
					that.$('[data-customfields="userPerms"]').html(new UserPermissionList({
						collection : that.userPermInputList,
						model : that.model,
						userList : that.userList,
						policyType 	: XAEnums.AssetType.ASSET_HDFS.value
					}).render().el);
			});
		},
	
		beforeSave : function(){
			//TODO FIXME remove the hard coding
			var that = this;
			this.model.set('assetId', this.assetModel.id);
			var permMapList = new VXPermMapList();
			
			this.formInputList.each(function(m){
				if(!_.isUndefined(m.get('groupId'))){
					var uniqueID = _.uniqueId(new Date()+':');
					_.each(m.get('groupId').split(","),function(groupId){
						_.each(m.get('_vPermList'), function(perm){
							var params = {
									//groupId: m.get('groupId'),
						//			id : perm.id,
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
							//		id : perm.id,
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
			
			if(!_.isUndefined(this.model.get('name')) && _.isUndefined(this.model.get('name').split(','))){
				this.model.set('name', _.unique(this.model.get('name').split(',')).join());
			}
			this.model.set('permMapList', permMapList);
			//this.model.set('_vPermMapList', permMapList);
			this.model.unset('userIdList');
			this.model.set('resourceType',XAEnums.ResourceType.RESOURCE_PATH.value);
			if(this.model.get('resourceStatus') != XAEnums.BooleanValue.BOOL_TRUE.value){
				this.model.set('resourceStatus', XAEnums.ActiveStatus.STATUS_DISABLED.value);
			}
			
			
		},
		/** all post render plugin initialization */
		initializePlugins: function(){
			var that= this;	
			function split( val ) {
				return val.split( /,\s*/ );
			}
			function extractLast( term ) {
				return split( term ).pop();
			}

			this.fields.name.editor.$el.bind( "keydown", function( event ) {
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
							dataSourceName: that.assetModel.get('name'),
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
								that.fields.name.editor.$el.tagit("createTag", tag);
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
					that.fields.name.$el.removeClass('error');
		        	that.fields.name.$el.find('.help-inline').html('');
					var tags =  [];
			        console.log(ui.tag);
				if(ui.tagLabel.lastIndexOf('/') < 0 || 
			        		ui.tagLabel.lastIndexOf('/') == ui.tagLabel.length -1 && ui.tagLabel.lastIndexOf('/') != 0){
			        	tags = ui.tagLabel.substr(0,ui.tagLabel.lastIndexOf('/'));
			        	that.fields.name.$el.addClass('error');
			        	that.fields.name.$el.find('.help-inline').html('Please enter valid resource path : ' + ui.tagLabel);
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
		evRecursiveChange : function(form, fieldEditor){
			XAUtil.checkDirtyFieldForToggle(fieldEditor);
		},
		evResourceStatusChange : function(form, fieldEditor){
			XAUtil.checkDirtyFieldForToggle(fieldEditor);
		}
	});

	return PolicyForm;
});
