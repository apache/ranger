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
    
	var Backbone	= require('backbone');
    	var App         = require('App');
	var XAEnums		= require('utils/XAEnums');
	var XAUtil			= require('utils/XAUtils');
	var localization	= require('utils/XALangSupport');
	var VXUser		= require('models/VXUser');
	require('bootstrap-editable');
    	
	var UserPermissionItem = Backbone.Marionette.ItemView.extend({
		_msvName : 'UserPermissionItem',
		template : require('hbs!tmpl/policies/UserPermItem'),
		tagName : 'tr',
		templateHelpers : function(){
						
			return {
				permissions 	: this.accessTypes,
				policyConditions: this.policyConditions,
//				policyStorm 	: this.policyType == XAEnums.ServiceType.Service_STORM.value ? true :false,
			   isModelNew		: !this.model.has('editMode'),
			   stormPerms		: this.stormPermsIds.length == 14 ? _.union(this.stormPermsIds,[-1]) : this.stormPermsIds
			};
		},
		ui : {
			selectUsers		: '[data-js="selectUsers"]',
			tags			: '[class=tags]'
		},
		events : {
			'click [data-action="delete"]'	: 'evDelete',
			'click td'						: 'evClickTD',
			'change [data-js="selectUsers"]': 'evSelectUser',
			'change input[class="policy-conditions"]'	: 'policyCondtionChange'
		},

		initialize : function(options) {
			_.extend(this, _.pick(options, 'userList','policyType','accessTypes','policyConditions'));
            //this.subjectList = this.mStudent.getSubjectList();
			this.stormPermsIds = [];
			if(this.policyType == XAEnums.AssetType.ASSET_STORM.value){
				if(this.model.has('editMode') && this.model.get('editMode')){
					this.stormPermsIds = _.map(this.model.get('_vPermList'), function(p){
						if(XAEnums.XAPermType.XA_PERM_TYPE_ADMIN.value != p.permType)
							return p.permType;
					});
				}
			}
		},
 
		onRender : function() {
			var that = this;
			
			this.accessItems = _.map(this.accessTypes, function(perm){ 
				if(!_.isUndefined(perm)) 
					return {'type':perm.label,isAllowed : false}
			});
			
			if(this.model.get('userName') != undefined){
				this.ui.selectUsers.val(this.model.get('userName').split(','));
			}
			if(!_.isUndefined(this.model.get('conditions'))){
				_.each(this.model.get('conditions'), function(obj){
					console.log(obj)
					this.$el.find('input[data-js="'+obj.type+'"]').val(obj.value.toString())
				},this);
			}
			
			if(this.model.has('editMode') && this.model.get('editMode')){
				_.each(this.model.get('accesses'), function(p){
					if(p.isAllowed){
						this.$el.find('input[data-name="' + p.type + '"]').attr('checked', 'checked');
						_.each(this.accessItems,function(obj){ if(obj.type == p.type) obj.isAllowed=true;})
					}
				},this);
			}
			if(this.policyType == XAEnums.AssetType.ASSET_STORM.value)
				this.renderStormPerms();
			
			this.createSelectUserDropDown();
			this.userDropDownChange();
			
		},
		checkDirtyFieldForDropDown : function(e){
			//that.model.has('groupId')
			var userIdList =[];
			if(!_.isUndefined(this.model.get('userId')))
				userIdList = this.model.get('userId').split(',');
			XAUtil.checkDirtyField(userIdList, e.val, $(e.currentTarget));
		},
		toggleAddButton : function(e){
			var temp = [];
			this.collection.each(function(m){
				if(!_.isUndefined(m.get('userId'))){
					temp.push.apply(temp, m.get('userId').split(','));
					
				}
			});
			if(!_.isUndefined(e)){
				if( !_.isUndefined(e.added) && ((temp.length + 1) == this.userList.length)) 
					$('[data-action="addUser"]').hide();
				if(!_.isUndefined(e.removed))
					$('[data-action="addUser"]').show();
			}else{
				$('[data-action="addUser"]').show();
			}
		},
		evDelete : function(){
			var that = this;
			this.collection.remove(this.model);
			this.toggleAddButton();
		},
		evClickTD : function(e){
			var that = this;
			var $el = $(e.currentTarget),permList =[],perms =[];
			if($(e.toElement).is('td')){
				var $checkbox = $el.find('input');
				$checkbox.is(':checked') ? $checkbox.prop('checked',false) : $checkbox.prop('checked',true);  
			}
//			var curPerm = $el.find('input').data('id');
			var curPermName = $el.find('input').data('name');
			if(!_.isUndefined(curPermName)){
				var perms = [];
				if(this.model.has('accesses')){
					if(_.isArray(this.model.get('accesses')))
						perms = this.model.get('accesses');
					else
						perms.push(this.model.get('accesses'));
				}
				
				
				if($el.find('input[type="checkbox"]').is(':checked')){
					_.each(that.accessItems, function(obj){ if(obj.type == curPermName) obj.isAllowed = true });
					/*if(curPermName == XAEnums.XAPermType.XA_PERM_TYPE_ADMIN.label){
						$el.parent().find('input[type="checkbox"]:not(:checked)[data-name!="'+curPermName+'"]').map(function(){
							_.each(that.accessItems, function(obj){ if(obj.type == $(this).data('name')) obj.isAllowed = true }, this);
						});
						//	this.model.set('_vPermList', perms);
						$el.parent().find('input[type="checkbox"]').prop('checked',true);
					}*/
				} else {
					_.each(that.accessItems, function(obj){ if(obj.type == curPermName ) obj.isAllowed = false }, this);
				}
//				this.checkDirtyFieldForCheckBox(perms);
				if(!_.isEmpty(that.accessItems)){
					this.model.set('accesses', that.accessItems);
				} else {
					this.model.unset('accesses');
				}
			}
		},
		checkDirtyFieldForCheckBox : function(perms){
			var permList = [];
			if(!_.isUndefined(this.model.get('_vPermList')))
				permList = _.map(this.model.attributes._vPermList,function(obj){return obj.permType;});
			perms = _.map(perms,function(obj){return obj.permType;});
			XAUtil.checkDirtyField(permList, perms, this.$el);
		},
		createSelectUserDropDown :function(){
			var that = this;
			if(this.model.has('editMode') && !_.isEmpty(this.ui.selectUsers.val())){
				var temp = this.ui.selectUsers.val().split(",");
				_.each(temp , function(id){
					if(_.isUndefined(that.userList.where({ name : name}))){
						var user = new VXUser({name: name});
						user.fetch({async:false}).done(function(){
							that.userList.add(user);
						});
					}
				});
			}
			this.userArr = this.userList.map(function(m){
				return { id : m.id+"" , text : m.get('name')};
			});
			this.ui.selectUsers.select2({
				closeOnSelect : true,
				placeholder : 'Select User',
		//		maximumSelectionSize : 1,
				width :'220px',
				tokenSeparators: [",", " "],
				tags : this.userArr, 
				initSelection : function (element, callback) {
					var data = [];
					$(element.val().split(",")).each(function () {
						var obj = _.findWhere(that.userArr,{ text : this });	
						data.push({ id : obj.id, text: this });
					});
					callback(data);
				},
				createSearchChoice: function(term, data) {
					/*if ($(data).filter(function() {
						return this.text.localeCompare(term) === 0;
						//return this.value == (term) ?  true : false;
					}).length === 0) {
						return {
							id : term,
							text: term
						};
					}*/
				},
				ajax: { 
					url: "service/xusers/users",
					dataType: 'json',
					data: function (term, page) {
						return {name : term};
					},
					results: function (data, page) { 
						var results = [],selectedVals=[];
						/*if(!_.isEmpty(that.ui.selectUsers.select2('val')))
							selectedVals = that.ui.selectUsers.select2('val');
						selectedVals.push.apply(selectedVals, that.getUsersSelectdValues());
						selectedVals = $.unique(selectedVals);*/
						selectedVals = that.getUsersSelectdValues();
						if(data.resultSize != "0"){
							//if(data.vXUsers.length > 1){

								results = data.vXUsers.map(function(m, i){	return {id : m.id+"", text: m.name};	});
								if(!_.isEmpty(selectedVals))
									results = XAUtil.filterResultByIds(results, selectedVals);
								//console.log(results.length);
								return {results : results};
						//	}
						//	results = [{id : data.vXUsers.id+"", text: data.vXUsers.name}];
						//	return {results : results};
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
					return 'No user found.';
				}
				
			}).on('select2-focus', XAUtil.select2Focus);
		},
		userDropDownChange : function(){
			var that = this;
			this.ui.selectUsers.on('change',function(e){
				//	console.log(e.currentTarget.value);
					that.checkDirtyFieldForDropDown(e);
					that.toggleAddButton(e);
					var duplicateUsername = false;
					if(e.removed != undefined){
						var gIdArr = [],gNameArr = [];
						gIdArr = _.without(that.model.get('userId').split(','), e.removed.id);
						if(that.model.get('userName') != undefined)
							gNameArr = _.without(that.model.get('userName').split(','), e.removed.text);
						if(!_.isEmpty(gIdArr)){
							that.model.set('userId',gIdArr.join(','));
							that.model.set('userName',gNameArr.join(','));
						}else{
							that.model.unset('userId');
							that.model.unset('userName');
						}
						return;
						
					}
					if(!_.isUndefined(e.added)){
							that.model.set('userId', e.currentTarget.value);
							var userNameList = _.map($(e.currentTarget).select2("data"), function(obj){return obj.text});
							that.model.set('userName',userNameList.toString())
					}
				});
		},
		getUsersSelectdValues : function(){
			var vals = [],selectedVals = [];
			this.collection.each(function(m){
				if(!_.isUndefined(m.get('userId'))){
					vals.push.apply(vals, m.get('userId').split(','));
				}
			});
			if(!_.isEmpty(this.ui.selectUsers.select2('val')))
				selectedVals = this.ui.selectUsers.select2('val');
			vals.push.apply(vals , selectedVals);
			vals = $.unique(vals);
			return vals;
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
		renderStormPerms :function(){
			var that = this;
			var permArr = _.pick(XAEnums.XAPermType,  XAUtil.getStormActions(this.policyType));
			this.stormPerms =  _.map(permArr,function(m){return {text:m.label, value:m.value};});
			this.stormPerms.push({'value' : -1, 'text' : 'Select/Deselect All'});
			this.ui.tags.editable({
			    placement: 'right',
//			    emptytext : 'Please select',
			    source: this.stormPerms,
			    display: function(idList,srcData) {
			    	if(_.isEmpty(idList.toString())){
			    		$(this).html('');
			    		return;
			    	}
			    	if(!_.isArray(idList))
			    		idList = [idList];
//			    	that.checkDirtyFieldForGroup(values);
			    	var permTypeArr = [];
		    		var valArr = _.map(idList, function(id){
		    			if(!(parseInt(id) <= 0) && (!_.isNaN(parseInt(id)))){
			    			var obj = _.findWhere(srcData,{'value' : parseInt(id)});
			    			permTypeArr.push({permType : obj.value});
			    			return "<span class='label label-inverse'>" + obj.text + "</span>";
		    			}
		    		});
		    		if(that.model.has('_vPermList')){
                        var adminPerm = _.where(that.model.get('_vPermList'),{'permType': XAEnums.XAPermType.XA_PERM_TYPE_ADMIN.value });
                        permTypeArr = _.isEmpty(adminPerm) ? permTypeArr : _.union(permTypeArr,adminPerm);
                    }
                    that.model.set('_vPermList', permTypeArr);
		    		
		    		$(this).html(valArr.join(" "));
			    },
			});
			this.$('[id^="tags-edit-"]').click(function(e) {
			    e.stopPropagation();
			    e.preventDefault();
			    that.$('#' + $(this).data('editable') ).editable('toggle');
			    that.$('input[type="checkbox"][value="-1"]').click(function(e){
					var checkboxlist =$(this).closest('.editable-checklist').find('input[type="checkbox"][value!=-1]');
					$(this).is(':checked') ? checkboxlist.prop('checked',true) : checkboxlist.prop('checked',false); 
					
				});
			});
		},

	});



	return Backbone.Marionette.CompositeView.extend({
		_msvName : 'UserPermissionList',
		template : require('hbs!tmpl/common/UserPermissionList'),
		//tagName : 'ul', 
		//className : 'timeline-container',
		templateHelpers :function(){
			return {
				permHeaders : this.getPermHeaders()
			};
		},
		getItemView : function(item){
			if(!item){
				return;
			}
			return UserPermissionItem;
		},
		itemViewContainer : ".js-formInput",
		itemViewOptions : function() {
			return {
				'collection' : this.collection,
				'userList' : this.userList,
				'policyType'	: this.policyType,
				'accessTypes' : this.accessTypes,
				'policyConditions' : this.rangerServiceDefModel.get('policyConditions')
			};
		},
		events : {
			'click [data-action="addUser"]' : 'addNew'
		},
		initialize : function(options) {
			_.extend(this, _.pick(options, 'userList','policyType','accessTypes','rangerServiceDefModel'));
			//this.listenTo(this.collection, 'removeAddBtn', this.render, this);
			/*if(options.model.isNew())
				this.collection.add(new Backbone.Model());*/
			if(this.collection.length == 0)
				this.collection.add(new Backbone.Model());
				
		},
		onRender : function(){
			//console.log("onRender of ArtifactFormNoteList called");
			this.toggleAddButton();
		},
		addNew : function(){
			var that =this;
			this.collection.add(new Backbone.Model());
			this.toggleAddButton();
		},
		toggleAddButton : function(){
			var userIds=[];
			this.collection.each(function(m){
				if(!_.isUndefined(m.get('userId'))){
					var temp = m.get('userId').split(',');
					userIds.push.apply(userIds,temp);
				}
			});
			if(userIds.length == this.userList.length)
				this.$('button[data-action="addUser"]').hide();
			else
				this.$('button[data-action="addUser"]').show();
		},
		getPermHeaders : function(){
			var permList = _.map(this.accessTypes,function(type){ return type.label});
			if(!_.isEmpty(this.rangerServiceDefModel.get('policyConditions'))){
				_.each(this.rangerServiceDefModel.get('policyConditions'), function(cond){
					if(!_.isNull(cond) && !_.isNull(cond.label)) permList.unshift(cond.label);
				});
			}
			permList.unshift(localization.tt('lbl.selectUser'));
			permList.push("");
			return permList;
		},
	
	});

});
