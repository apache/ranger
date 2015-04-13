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
	var XAUtil			= require('utils/XAUtils');
	var localization	= require('utils/XALangSupport');
	var VXGroup			= require('models/VXGroup');
	var VXGroupList			= require('collections/VXGroupList');
	var VXUserList			= require('collections/VXUserList');
	require('bootstrap-editable');
    	
	var FormInputItem = Backbone.Marionette.ItemView.extend({
		_msvName : 'FormInputItem',
		template : require('hbs!tmpl/policies/PermissionItem'),
		tagName : 'tr',
		templateHelpers : function(){
			
			return {
				permissions 	: this.accessTypes,
				policyConditions: this.policyConditions,
				isModelNew		: !this.model.has('editMode'),
				perms			: this.permsIds.length == 14 ? _.union(this.permsIds,[-1]) : this.permsIds,
			};
		},
		ui : {
			selectGroups	: '[data-js="selectGroups"]',
			selectUsers		: '[data-js="selectUsers"]',
			addPerms		: 'a[data-js="permissions"]',
			conditionsTags	: '[class=tags1]',
			delegatedAdmin	: 'input[data-js="delegatedAdmin"]',
			addPermissionsSpan : '.add-permissions',
			addConditionsSpan : '.add-conditions',
		},
		events : {
			'click [data-action="delete"]'	: 'evDelete',
			'click td'						: 'evClickTD',
			'change [data-js="selectGroups"]': 'evSelectGroup',
			'change [data-js="selectUsers"]': 'evSelectUser',
			'change input[class="policy-conditions"]'	: 'policyCondtionChange'
		},

		initialize : function(options) {
			_.extend(this, _.pick(options, 'groupList','policyType','accessTypes','policyConditions','userList'));
			this.setupPermissionsAndConditions();
			
		},
 
		onRender : function() {
			//To setup permissions for edit mode 
			this.setupFormForEditMode();
			//create select2 dropdown for groups and users  
			this.createDropDown(this.ui.selectGroups, this.groupList, true);
			this.createDropDown(this.ui.selectUsers, this.userList, false);
			//groups or users select2 dropdown change vent 
			
			this.dropDownChange(this.ui.selectGroups);
			this.dropDownChange(this.ui.selectUsers);
			//render permissions and policy conditions
			this.renderPerms();
			this.renderPolicyCondtion();
		},
		setupFormForEditMode : function() {
			this.accessItems = _.map(this.accessTypes, function(perm){ 
				if(!_.isUndefined(perm)) 
					return {'type':perm.name,isAllowed : false}
			});
			if(this.model.has('editMode') && this.model.get('editMode')){
				if(!_.isUndefined(this.model.get('groupName')) && !_.isNull(this.model.get('groupName'))){
					this.ui.selectGroups.val(this.model.get('groupName').split(','));
				}
				if(!_.isUndefined(this.model.get('userName')) && !_.isNull(this.model.get('userName'))){
					this.ui.selectUsers.val(this.model.get('userName').split(','));
				}
				
				if(!_.isUndefined(this.model.get('conditions'))){
					_.each(this.model.get('conditions'), function(obj){
						this.$el.find('input[data-js="'+obj.type+'"]').val(obj.values.toString())
					},this);
				}
				_.each(this.model.get('accesses'), function(p){
					if(p.isAllowed){
						this.$el.find('input[data-name="' + p.type + '"]').attr('checked', 'checked');
						_.each(this.accessItems,function(obj){ if(obj.type == p.type) obj.isAllowed=true;})
					}
				},this);
				
				if(!_.isUndefined(this.model.get('delegateAdmin')) && this.model.get('delegateAdmin')){
					this.ui.delegatedAdmin.attr('checked', 'checked');
				}
			}
		},
		setupPermissionsAndConditions : function() {
			var that = this;
			this.permsIds = [], this.conditions = {};
			//Set Permissions obj
			if( this.model.has('editMode') && this.model.get('editMode')){
				_.each(this.model.get('accesses'), function(p){
					if(p.isAllowed){
						var access = _.find(that.accessTypes,function(obj){if(obj.name == p.type) return obj});
						this.permsIds.push(access.name);
					}
					
				}, this);
				//Set PolicyCondtion Obj to show in edit mode
				_.each(this.model.get('conditions'), function(p){
					this.conditions[p.type] = p.values;
				}, this);
			}
		},
		dropDownChange : function($select){
			var that = this;
			$select.on('change',function(e){
		//		console.log(e.currentTarget.value);
				var name = ($(e.currentTarget).attr('data-js') == that.ui.selectGroups.attr('data-js')) ? 'group': 'user';
				that.checkDirtyFieldForDropDown(e);
				
				if(e.removed != undefined){
					var gNameArr = [];
					if(that.model.get(name+'Name') != undefined)
						gNameArr = _.without(that.model.get(name+'Name').split(','), e.removed.text);
					if(!_.isEmpty(gNameArr)){
						that.model.set(name+'Name',gNameArr.join(','));
					}else{
						that.model.unset(name+'Name');
					}
					that.toggleAddButton(e);
					return;
				}
				if(!_.isUndefined(e.added)){
					var nameList = _.map($(e.currentTarget).select2("data"), function(obj){return obj.text});
					that.model.set(name+'Name',nameList.toString());
					that.toggleAddButton(e);
				}
			});
		},
		createDropDown :function($select, list, typeGroup){
			var that = this;
			var placeholder = (typeGroup) ? 'Select Group' : 'Select User';
			var url 		= (typeGroup) ? "service/xusers/groups" : "service/xusers/users";
			if(this.model.has('editMode') && !_.isEmpty($select.val())){
				var temp = $select.val().split(",");
				_.each(temp , function(name){
					if(_.isEmpty(list.where({ 'name' : name}))){
						var coll;
						coll = typeGroup ? new VXGroupList() : new VXUserList();
						coll.queryParams['name'] = name;
						coll.fetch({async:false}).done(function(){
							list.add(coll.models);
						});
					}
				});
			}
			var tags = list.map(function(m){
				return { id : m.id+"" , text : m.get('name')};
			});
			$select.select2({
				closeOnSelect : true,
				placeholder : placeholder,
			//	maximumSelectionSize : 1,
				width :'220px',
				tokenSeparators: [",", " "],
				tags : tags, 
				initSelection : function (element, callback) {
					var data = [];
					console.log(list);
					
					$(element.val().split(",")).each(function () {
						var obj = _.findWhere(tags,{text:this});
						data.push({id: obj.id, text: this})
					});
					callback(data);
				},
				ajax: { 
					url: url,
					dataType: 'json',
					data: function (term, page) {
						return {name : term, isVisible : XAEnums.VisibilityStatus.STATUS_VISIBLE.value};
					},
					results: function (data, page) { 
						var results = [] , selectedVals = [];
						//Get selected values of groups/users dropdown
						selectedVals = that.getSelectdValues($select, typeGroup);
						if(data.resultSize != "0"){
							if(typeGroup)
								results = data.vXGroups.map(function(m, i){	return {id : m.id+"", text: m.name};	});
							else
								results = data.vXUsers.map(function(m, i){	return {id : m.id+"", text: m.name};	});
							if(!_.isEmpty(selectedVals))
								results = XAUtil.filterResultByText(results, selectedVals);
							return {results : results};
						}
						return {results : results};
					}
				},	
				formatResult : function(result){
					return result.text;
				},
				formatSelection : function(result){
					return result.text;
				},
				formatNoMatches: function(result){
					return typeGroup ? 'No group found.' : 'No user found.';
				}
			}).on('select2-focus', XAUtil.select2Focus);
		},
		renderPerms :function(){
			var that = this;
			this.perms =  _.map(this.accessTypes,function(m){return {text:m.label, value:m.name};});
			this.perms.push({'value' : -1, 'text' : 'Select/Deselect All'});
			//create x-editable for permissions
			this.ui.addPerms.editable({
			    emptytext : 'Add Permissions',
				source: this.perms,
				value : this.permsIds,
				display: function(values,srcData) {
					if(_.isNull(values) || _.isEmpty(values)){
						$(this).empty();
						that.model.unset('accesses');
						that.ui.addPermissionsSpan.find('i').attr('class', 'icon-plus');
						that.ui.addPermissionsSpan.attr('title','add');
						return;
					}
					if(_.contains(values,"-1")){
						values = _.without(values,"-1")
					}
//			    	that.checkDirtyFieldForGroup(values);
					
					
					var permTypeArr = [];
					var valArr = _.map(values, function(id){
						if(!_.isUndefined(id)){
							var obj = _.findWhere(srcData,{'value' : id});
							permTypeArr.push({permType : obj.value});
							return "<span class='label label-info'>" + obj.text + "</span>";
						}
					});
					var perms = []
					if(that.model.has('accesses')){
							perms = that.model.get('accesses');
					}
					
					var items=[];
					_.each(that.accessItems, function(item){ 
						if($.inArray( item.type, values) >= 0){
							item.isAllowed = true;
							items.push(item) ;
						}
					},this);
					// Save form data to model
					that.model.set('accesses', items);
					
					$(this).html(valArr.join(" "));
					that.ui.addPermissionsSpan.find('i').attr('class', 'icon-pencil');
					that.ui.addPermissionsSpan.attr('title','edit');
				},
			}).on('click', function(e) {
				e.stopPropagation();
				e.preventDefault();
				that.clickOnPermissions(that);
			});
			that.ui.addPermissionsSpan.click(function(e) {
				e.stopPropagation();
				that.$('a[data-js="permissions"]').editable('toggle');
				that.clickOnPermissions(that);
			});
			
		},
		clickOnPermissions : function(that) {
			var selectAll = true;
			var checklist = that.$('.editable-checklist').find('input[type="checkbox"]')
			_.each(checklist,function(checkbox){ if($(checkbox).val() != -1 && !$(checkbox).is(':checked')) selectAll = false;})
			if(selectAll){
				that.$('.editable-checklist').find('input[type="checkbox"][value="-1"]').prop('checked',true)
			}else{
				that.$('.editable-checklist').find('input[type="checkbox"][value="-1"]').prop('checked',false)
			}
			//for selectAll functionality
			that.$('input[type="checkbox"][value="-1"]').click(function(e){
				var checkboxlist =$(this).closest('.editable-checklist').find('input[type="checkbox"][value!=-1]')
				$(this).is(':checked') ? checkboxlist.prop('checked',true) : checkboxlist.prop('checked',false); 
				
			});
		},
		renderPolicyCondtion : function() {
			var that = this;
			
			if(this.policyConditions.length > 0){
				var tmpl = _.map(this.policyConditions,function(obj){ 
					return '<div class="editable-address margin-bottom-5"><label style="display:block !important;"><span>'+obj.label+' : </span></label><input type="text" name="'+obj.name+'" ></div>'
				});
				//Create new bootstrap x-editable `policyConditions` dataType for policy conditions 
				XAUtil.customXEditableForPolicyCond(tmpl.join(''));
				//create x-editable for policy conditions
				this.$('#policyConditions').editable({
					emptytext : 'Add Conditions',
					value : this.conditions, 
					display: function(value) {
						var continue_ = false, i = 0;
						if(!value) {
							$(this).empty();
							return; 
						}
						_.each(value, function(val, name){ if(!_.isEmpty(val)) continue_ = true; });
						if(continue_){
							var html = _.map(value, function(val,name) {
								var label = (i%2 == 0) ? 'label label-inverse' : 'label';
								i++;
								return _.isEmpty(val) ? '' : '<span class="'+label+'">'+name+' : '+ val + '</span>';	
							});
							var cond = _.map(value, function(val, name) {
								return {'type' : name, 'values' : !_.isArray(val) ?  val.split(',') : val};
							});
							that.model.set('conditions', cond);
							$(this).html(html);
							that.ui.addConditionsSpan.find('i').attr('class', 'icon-pencil');
							that.ui.addConditionsSpan.attr('title','edit');
						}else{
							that.model.unset('conditions');
							$(this).empty();
							that.ui.addConditionsSpan.find('i').attr('class', 'icon-plus');
							that.ui.addConditionsSpan.attr('title','add');
						}
					}
				});
				that.ui.addConditionsSpan.click(function(e) {
					e.stopPropagation();
					that.$('#policyConditions').editable('toggle');
				});
				
			}
		},
		getSelectdValues : function($select, typeGroup){
			var vals = [],selectedVals = [];
			var name = typeGroup ? 'group' : 'user';
			this.collection.each(function(m){
				if(!_.isUndefined(m.get(name+'Name')) && !_.isNull(m.get(name+'Name'))){
					vals.push.apply(vals, m.get(name+'Name').split(','));
				}
			});
			if(!_.isEmpty($select.select2('val')))
				selectedVals = $select.select2('val');
			vals.push.apply(vals , selectedVals);
			vals = $.unique(vals);
			return vals;
		},
		evDelete : function(){
			var that = this;
			this.collection.remove(this.model);
			this.toggleAddButton();
		},
		evClickTD : function(e){
			var $el = $(e.currentTarget);
			XAUtil.checkDirtyFieldForToggle($el);
			//Set Delegated Admin value 
			if(!_.isUndefined($el.find('input').data('js'))){
				this.model.set('delegateAdmin',$el.find('input').is(':checked'))
				return;
			}
		},
		checkDirtyFieldForCheckBox : function(perms){
			var permList = [];
			if(!_.isUndefined(this.model.get('_vPermList')))
				permList = _.map(this.model.attributes._vPermList,function(obj){return obj.permType;});
			perms = _.map(perms,function(obj){return obj.permType;});
			XAUtil.checkDirtyField(permList, perms, this.$el);
		},
		toggleAddButton : function(e){
			var grpTemp = [], usrTemp = [];
			this.collection.each(function(m){
				if(!_.isUndefined(m.get('groupName')) && !_.isNull(m.get('groupName'))){
					grpTemp.push.apply(grpTemp, m.get('groupName').split(','));
				}
				if(!_.isUndefined(m.get('userName')) && !_.isNull(m.get('userName'))){
					usrTemp.push.apply(usrTemp, m.get('userName').split(','));
				}	
			});
			if(!_.isUndefined(e)){
				if( !_.isUndefined(e.added)){
					if((grpTemp.length ) == this.groupList.length && ((usrTemp.length) == this.userList.length)){
						$('[data-action="addGroup"]').hide();
					}else{
						$('[data-action="addGroup"]').show();
					}
				} 
				if(!_.isUndefined(e.removed))
					$('[data-action="addGroup"]').show();
			}else{
				if((grpTemp.length ) == this.groupList.length && ((usrTemp.length) == this.userList.length)){
					$('[data-action="addGroup"]').hide();
				}else{
					$('[data-action="addGroup"]').show();
				}
			}
		},
		policyCondtionChange :function(e){
			if(!_.isEmpty($(e.currentTarget).val()) && !_.isEmpty(this.policyConditions)){
				var policyCond = { 'type' : $(e.currentTarget).attr('data-js'), 'value' : $(e.currentTarget).val() } ;
				var conditions = [];
				if(this.model.has('conditions')){
					conditions = this.model.get('conditions')
				}
				conditions.push(policyCond);
				this.model.set('conditions',conditions);
			}
				
		},
		checkDirtyFieldForDropDown : function(e){
			//that.model.has('groupId')
			var groupIdList =[];
			if(!_.isUndefined(this.model.get('groupId')))
				groupIdList = this.model.get('groupId').split(',');
			XAUtil.checkDirtyField(groupIdList, e.val, $(e.currentTarget));
		},
	});



	return Backbone.Marionette.CompositeView.extend({
		_msvName : 'FormInputItemList',
		template : require('hbs!tmpl/policies/PermissionList'),
		templateHelpers :function(){
			return {
				permHeaders : this.getPermHeaders()
			};
		},
		getItemView : function(item){
			if(!item){
				return;
			}
			return FormInputItem;
		},
		itemViewContainer : ".js-formInput",
		itemViewOptions : function() {
			return {
				'collection' 	: this.collection,
				'groupList' 	: this.groupList,
				'userList' 	: this.userList,
				'policyType'	: this.policyType,
				'accessTypes'	: this.accessTypes,
				'policyConditions' : this.rangerServiceDefModel.get('policyConditions')
			};
		},
		events : {
			'click [data-action="addGroup"]' : 'addNew'
		},
		initialize : function(options) {
			_.extend(this, _.pick(options, 'groupList','policyType','accessTypes','rangerServiceDefModel','userList'));
			this.listenTo(this.groupList, 'sync', this.render, this);
			if(this.collection.length == 0)
				this.collection.add(new Backbone.Model());
		},
		onRender : function(){
//			this.toggleAddButton();
		},
		addNew : function(){
			var that =this;
//			if(this.groupList.length > this.collection.length && (this.userList.length > this.collection.length)){
				this.collection.add(new Backbone.Model());
//				this.toggleAddButton();
//			}
		},
		toggleAddButton : function(){
			var groupNames=[], userNames=[];
			this.collection.each(function(m){
				if(!_.isUndefined(m.get('groupName'))){
					var temp = m.get('groupName').split(',');
					groupNames.push.apply(groupNames,temp);
				}
				if(!_.isUndefined(m.get('userName'))){
					var temp = m.get('userName').split(',');
					userNames.push.apply(userNames,temp);
				}
			});
			if(groupNames.length == this.groupList.length && userNames.length == this.userList.length )
				this.$('button[data-action="addGroup"]').hide();
			else
				this.$('button[data-action="addGroup"]').show();
		},
		getPermHeaders : function(){
			var permList = [];
			permList.unshift(localization.tt('lbl.delegatedAdmin'));
			permList.unshift(localization.tt('lbl.permissions'));
			if(!_.isEmpty(this.rangerServiceDefModel.get('policyConditions'))){
				permList.unshift(localization.tt('h.policyCondition'));
			}
			permList.unshift(localization.tt('lbl.selectUser'));
			permList.unshift(localization.tt('lbl.selectGroup'));
			permList.push("");
			return permList;
		},
	});

});
