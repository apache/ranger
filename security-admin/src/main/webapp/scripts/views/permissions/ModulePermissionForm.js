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

 /*
 *
 */
define(function(require) {
    'use strict';

	var Backbone		= require('backbone');
    var App		        = require('App');
	var XAEnums			= require('utils/XAEnums');
	var XALinks 		= require('modules/XALinks');
	var XAUtil			= require('utils/XAUtils');
	var localization	= require('utils/XALangSupport');
	var VXGroup			= require('models/VXGroup');
	var VXGroupList		= require('collections/VXGroupList');
	var VXUserList		= require('collections/VXUserList');
	var VXModuleDef			= require('models/VXModuleDef');
	var VXModuleDefList		= require('collections/VXModuleDefList');
	var BackboneFormDataType	= require('models/BackboneFormDataType');
	require('bootstrap-editable');
	require('backbone-forms');
	require('backbone-forms.list');
	require('backbone-forms.templates');
	require('backbone-forms.XAOverrides');

	var ModulePermissionForm = Backbone.Form.extend({

		_viewName : 'ModulePermissionForm',
		template : require('hbs!tmpl/permissions/ModulePermissionForm_tmpl'),
		templateHelpers :function(){
		},
		templateData : function(){
			return { 'id' : this.model.id, 'permHeaders' : this.getPermHeaders() };
		},
		initialize : function(options) {
			_.extend(this, _.pick(options, 'groupList','userList'));
			if (!this.model.isNew()){
				this.setupFieldsforEditModule();
			}
			Backbone.Form.prototype.initialize.call(this, options);
		},
		ui : {
			/*selectGroups	: 'div[data-fields="selectGroups"]',
			selectUsers		: 'div[data-fields="selectUsers"]',*/
		},
		events : {
		},
		/** fields for the form
		*/
		fields: ['module', 'selectGroups','selectUsers','isAllowed'],
		schema :function(){
			return this.getSchema();
		},
		getSchema : function(){
			var that = this;
			return {
				module : {
					type		: 'Text',
					title		: localization.tt("lbl.moduleName") +' *',
					editorAttrs : {'readonly' :'readonly'},
					validation	: {'required': true},
				},
				selectGroups : {
					type : 'Select2Remote',
					editorAttrs  : {'placeholder' :'Select Group','tokenSeparators': [",", " "],multiple:true},
					pluginAttr: this.getPlugginAttr(true,{'lookupURL':"service/xusers/groups",'permList':that.model.get('groupPermList'),'idKey':'groupId','textKey':'groupName'}),
					title : localization.tt('lbl.selectGroup')+' *'
				},
				selectUsers : {
					type : 'Select2Remote',
					editorAttrs  : {'placeholder' :'Select User','tokenSeparators': [",", " "],multiple:true},
					pluginAttr: this.getPlugginAttr(true,{'lookupURL':"service/xusers/users",'permList':that.model.get('userPermList'),'idKey':'userId','textKey':'userName'}),
					title : localization.tt('lbl.selectUser')+' *',
				},
				isAllowed : {
					type : 'Checkbox',
					editorAttrs  : {'checked':'checked',disabled:true},
					title : 'Is Allowed ?'
					},
			}
		},
		render: function(options) {
			var that = this;
			Backbone.Form.prototype.render.call(this, options);
			
		},
		setupFieldsforEditModule : function(){
			var groupsNVList=[],usersNVList =[];
			groupsNVList = _.map(this.model.get('groupPermList'),function(gPerm){
				return {'id': Number(gPerm.groupId), 'text':_.escape(gPerm.groupName)};
			});
			this.model.set('selectGroups', groupsNVList);

			usersNVList = _.map(this.model.get('userPermList'),function(uPerm){
				return {'id': Number(uPerm.userId), 'text':_.escape(uPerm.userName)};
			});
			this.model.set('selectUsers', usersNVList);

		},
		getPermHeaders : function(){
			var permList = [];
			permList.unshift(localization.tt('lbl.allowAccess'));
			permList.unshift(localization.tt('lbl.selectUser'));
			permList.unshift(localization.tt('lbl.selectGroup'));
//			permList.push("");
			return permList;
		},
		getPlugginAttr :function(autocomplete, options){
			var that = this;
			if(!autocomplete)
				return{ tags : true, width :'220px', multiple: true, minimumInputLength: 1 };
			else {
				return {
					closeOnSelect : true,
					multiple: true,
					minimumInputLength: 0,
					tokenSeparators: [",", " "],
					initSelection : function (element, callback) {
						var data = [];
						_.each(options.permList,function (elem) {
							data.push({id: elem[options.idKey], text: _.escape(elem[options.textKey])});
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
						type : 'GET',
						params : {
							timeout: 3000,
							contentType: "application/json; charset=utf-8",
						},
						cache: false,
						data: function (term, page) {
							//To be checked
							return { name : term, isVisible : XAEnums.VisibilityStatus.STATUS_VISIBLE.value };
						},
						results: function (data, page) {
							var results = [];
							var results = [], selectedVals = [];
							//Get selected values of groups/users dropdown
							selectedVals = that.getSelectedValues(options);
							if(data.resultSize != "0"){
								if(!_.isUndefined(data.vXGroups)){
									results = data.vXGroups.map(function(m, i){	return {id : m.id+"", text: _.escape(m.name) };	});
								} else if(!_.isUndefined(data.vXUsers)){
									results = data.vXUsers.map(function(m, i){	return {id : m.id+"", text: _.escape(m.name) };	});
									if(!_.isEmpty(selectedVals)){
										results = XAUtil.filterResultByText(results, selectedVals);
									}
								}
							}
							return { results : results};
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
						return options.textKey == 'groupName' ?  'No group found.' : 'No user found.'; 
					}
				};
			}
		},
		getSelectedValues : function(options){
			var vals = [],selectedVals = [];
			var type = options.textKey == 'groupName' ? 'selectGroups' : 'selectUsers';
			var $select = this.$('[name="'+type+'"]');
			if(!_.isEmpty($select.select2('data'))){
				selectedVals = _.map($select.select2('data'),function(obj){ return obj.text; });
			}
			vals.push.apply(vals , selectedVals);
			vals = $.unique(vals);
			return vals;
		},
		beforeSaveModulePermissions : function(){
			if(this.model.get('module') != ''){
				var groupValStr = this.fields.selectGroups.getValue();
				var userValStr = this.fields.selectUsers.getValue();
				this.compareAndUpdateObj(groupValStr,{'mode':'groups','permList':this.model.get('groupPermList'),'idKey':'groupId','textKey':'groupName'});
				this.compareAndUpdateObj(userValStr,{'mode':'users','permList':this.model.get('userPermList'),'idKey':'userId','textKey':'userName'});
			}
			return true;
		},
		compareAndUpdateObj: function(objValsStr,options){

			var selectedVals = (!_.isNull(objValsStr)) ? objValsStr.toString().split(',') : [];
			var selectedIdList=[];
			selectedVals = _.each(selectedVals, function(eachVal){
				//Ignoring any non existing Group Name
				if(_.isNumber(parseInt(eachVal))  && !_.isNaN(parseInt(eachVal))){
					selectedIdList.push(Number(eachVal));
				}
			});
			var modelPermList = options.permList;
			var modelPerms = _.unique(_.pluck(options.permList, options.idKey));
			if(!_.isEmpty(selectedIdList)){
				//Look for equals
				if(_.isEqual(selectedIdList,modelPerms)) {
					//No changes in Selected Users
				} else {
					//look for new values -
					//loop through each new element and check if it has any non matching ids
					var diff = _.filter(selectedIdList, function(value){ return !_.contains(modelPerms, value); });
					var that = this;
					if(!_.isEmpty(diff)){
						//push new elements to model groupPermList
						_.each(diff, function(newEl){
							var newObj = {};
							newObj[options.idKey] = newEl;
							newObj['moduleId'] = that.model.get('id');
							newObj['isAllowed'] = 1;
							options.permList.push(newObj);
						});
					}
					//Look for removed users/groups
					//loop through each model element and check new selected groups is missing from any original list  of group ids
					var updDiff = _.filter(modelPerms, function(value){ return !_.contains(selectedIdList, value); });
					if(!_.isEmpty(updDiff)){
						_.each(options.permList, function(origElem){
							if(_.contains(updDiff, origElem[options.idKey]))
								origElem.isAllowed = 0;
						});
					}
				}

			} else {
				//Remove permissions from all objects which earlier had permission
				_.each(options.permList, function(perm){
					perm.isAllowed = 0;
				});
			}

		}
	});
	
	return ModulePermissionForm;
});
