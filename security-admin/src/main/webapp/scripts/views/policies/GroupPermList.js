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
	require('bootstrap-editable');
    	
	var FormInputItem = Backbone.Marionette.ItemView.extend({
		_msvName : 'FormInputItem',
		template : require('hbs!tmpl/policies/GroupPermItem'),
		tagName : 'tr',
		templateHelpers : function(){
			
			
			return {
				permissions 	: this.getPerms(),
				policyKnox 		: this.policyType == XAEnums.ServiceType.Service_KNOX.value ? true :false,
//				policyStorm 	: this.policyType == XAEnums.ServiceType.Service_STORM.value ? true :false,
				isModelNew		: !this.model.has('editMode'),
				stormPerms		: this.stormPermsIds.length == 14 ? _.union(this.stormPermsIds,[-1]) : this.stormPermsIds  
						
			};
		},
		ui : {
			selectGroups	: '[data-js="selectGroups"]',
			inputIPAddress	: '[data-js="ipAddress"]',
			tags			: '[class=tags]'
		},
		events : {
			'click [data-action="delete"]'	: 'evDelete',
			'click td'						: 'evClickTD',
			'change [data-js="selectGroups"]': 'evSelectGroup',
			'change [data-js="ipAddress"]'	: 'evIPAddress'
		},

		initialize : function(options) {
			_.extend(this, _.pick(options, 'groupList','policyType','accessTypes'));
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
			if(!_.isUndefined(this.model.get('groupName'))){
				this.ui.selectGroups.val(this.model.get('groupName').split(','));
			}
			if(!_.isUndefined(this.model.get('ipAddress'))){
				this.ui.inputIPAddress.val(this.model.get('ipAddress').toString());
			}
			if(this.model.has('editMode') && this.model.get('editMode')){
				_.each(this.model.get('accesses'), function(p){
					if(p.value)
						this.$el.find('input[data-name="' + p.type + '"]').attr('checked', 'checked');
				},this);
			}
			this.createGroupDropDown();
			this.groupDropDownChange();
			if(this.policyType == XAEnums.AssetType.ASSET_STORM.value){
				this.renderStormPerms();
			}
			
			
			this.accessItems = _.map(this.getPerms(), function(perm){ return {'type':perm.label,value : false}});
		},
		groupDropDownChange : function(){
			var that = this;
			this.ui.selectGroups.on('change',function(e){
		//		console.log(e.currentTarget.value);
				that.checkDirtyFieldForDropDown(e);
				var duplicateGroupName = false;
				
				that.toggleAddButton(e);
				if(e.removed != undefined){
					var gIdArr = [],gNameArr = [];
					gIdArr = _.without(that.model.get('groupId').split(','), e.removed.id);
					if(that.model.get('groupName') != undefined)
						gNameArr = _.without(that.model.get('groupName').split(','), e.removed.text);
					if(!_.isEmpty(gIdArr)){
						that.model.set('groupId',gIdArr.join(','));
						that.model.set('groupName',gNameArr.join(','));
					}else{
						that.model.unset('groupId');
						that.model.unset('groupName');
					}
					return;
				}
				if(!_.isUndefined(e.added)){
						that.model.set('groupId', e.currentTarget.value);
						var groupNameList = _.map($(e.currentTarget).select2("data"), function(obj){return obj.text});
						that.model.set('groupName',groupNameList.toString())
				}
			});
		},
		createGroupDropDown :function(){
			var that = this;
			if(this.model.has('editMode') && !_.isEmpty(this.ui.selectGroups.val())){
				var temp = this.ui.selectGroups.val().split(",");
				_.each(temp , function(name){
					if(_.isUndefined(that.groupList.where({ name : name}))){
						var group = new VXGroup({name: name});
						group.fetch({async:false}).done(function(){
							that.groupList.add(group);
						});
					}
				});
			}
			this.groupArr = this.groupList.map(function(m){
				return { id : m.id+"" , text : m.get('name')};
			});
			this.ui.selectGroups.select2({
				closeOnSelect : true,
				placeholder : 'Select Group',
			//	maximumSelectionSize : 1,
				width :'220px',
				tokenSeparators: [",", " "],
				tags : this.groupArr, 
				initSelection : function (element, callback) {
					var data = [];
					console.log(that.groupList);
					
					$(element.val().split(",")).each(function () {
						var obj = _.findWhere(that.groupArr,{text:this});
						data.push({id: obj.id, text: this})
					});
					callback(data);
				},
				createSearchChoice: function(term, data) {
				/*	if ($(data).filter(function() {
						return this.text.localeCompare(term) === 0;
					}).length === 0) {
						return {
							id : term,
							text: term
						};
					}*/
				},
				ajax: { 
					url: "service/xusers/groups",
					dataType: 'json',
					data: function (term, page) {
						return {name : term};
					},
					results: function (data, page) { 
						var results = [] , selectedVals = [];
						/*if(!_.isEmpty(that.ui.selectGroups.select2('val')))
							selectedVals = that.ui.selectGroups.select2('val');*/
						selectedVals = that.getGroupSelectdValues();
						if(data.resultSize != "0"){
							//if(data.vXGroups.length > 1){

								results = data.vXGroups.map(function(m, i){	return {id : m.id+"", text: m.name};	});
								if(!_.isEmpty(selectedVals))
									results = XAUtil.filterResultByIds(results, selectedVals);
						//		console.log(results.length);
								return {results : results};
							//}
						//	results = [{id : data.vXGroups.id+"", text: data.vXGroups.name}];
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
					return 'No group found.';
				}
			}).on('select2-focus', XAUtil.select2Focus);
		},
		getGroupSelectdValues : function(){
			var vals = [],selectedVals = [];
			this.collection.each(function(m){
				if(!_.isUndefined(m.get('groupId'))){
					vals.push.apply(vals, m.get('groupId').split(','));
				}
			});
			if(!_.isEmpty(this.ui.selectGroups.select2('val')))
				selectedVals = this.ui.selectGroups.select2('val');
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
			var that = this;
			var $el = $(e.currentTarget),permList =[],perms =[];
			if($(e.toElement).is('td')){
				var $checkbox = $el.find('input');
				$checkbox.is(':checked') ? $checkbox.prop('checked',false) : $checkbox.prop('checked',true);
			}
			var curPerm = $el.find('input').data('id');
			var curPermName = $el.find('input').data('name');
			if(!_.isUndefined(curPerm)){
				var perms = [];
				if(this.model.has('accesses')){
					if(_.isArray(this.model.get('accesses')))
						perms = this.model.get('accesses');
					else
						perms.push(this.model.get('accesses'));
				}
				if($el.find('input[type="checkbox"]').is(':checked')){
					_.each(that.accessItems, function(obj){ if(obj.type == curPermName) obj.value = true });
					
					if(curPerm == XAEnums.XAPermType.XA_PERM_TYPE_ADMIN.value){
						$el.parent().find('input[type="checkbox"]:not(:checked)[data-id!="'+curPerm+'"]').map(function(){
							_.each(that.accessItems, function(obj){ if(obj.type == $(this).data('name')) obj.value = true }, this);
						});
						$el.parent().find('input[type="checkbox"]').prop('checked',true);
					}
				} else {
					_.each(that.accessItems, function(obj){ if(obj.type == curPermName ) obj.value = false }, this);
				}
				
//				this.checkDirtyFieldForCheckBox(perms);
				if(!_.isEmpty(that.accessItems))
					this.model.set('accesses', that.accessItems);
				else 
					this.model.unset('accesses');
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
			var temp = [];
			this.collection.each(function(m){
				if(!_.isUndefined(m.get('groupId'))){
					temp.push.apply(temp, m.get('groupId').split(','));
					
				}
			});
			if(!_.isUndefined(e)){
				if( !_.isUndefined(e.added) && ((temp.length + 1) == this.groupList.length)) 
					$('[data-action="addGroup"]').hide();
				if(!_.isUndefined(e.removed))
					$('[data-action="addGroup"]').show();
			}else{
				$('[data-action="addGroup"]').show();
			}
		},
		evIPAddress :function(e){
			if(!_.isEmpty($(e.currentTarget).val()))
				this.model.set('ipAddress',$(e.currentTarget).val().split(','));
			else
				this.model.unset('ipAddress');
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
//		    		if(!_.isEmpty(perms))
//		    			that.model.set('_vPermList', perms);
//		    		that.model.set('_vPermList', permTypeArr);
		    		$(this).html(valArr.join(" "));
			    },
			});
			this.$('[id^="tags-edit-"]').click(function(e) {
			    e.stopPropagation();
			    e.preventDefault();
			    that.$('#' + $(this).data('editable') ).editable('toggle');
			    that.$('input[type="checkbox"][value="-1"]').click(function(e){
					var checkboxlist =$(this).closest('.editable-checklist').find('input[type="checkbox"][value!=-1]')
					$(this).is(':checked') ? checkboxlist.prop('checked',true) : checkboxlist.prop('checked',false); 
					
				});
			});
			
		},
		checkDirtyFieldForDropDown : function(e){
			//that.model.has('groupId')
			var groupIdList =[];
			if(!_.isUndefined(this.model.get('groupId')))
				groupIdList = this.model.get('groupId').split(',');
			XAUtil.checkDirtyField(groupIdList, e.val, $(e.currentTarget));
		},
		getPerms : function(){
			var permList = _.map(this.accessTypes,function(type){ return type.label});
			return _.map(permList, function(perm){ return _.findWhere(XAEnums.XAPermType,{label:perm})})
		}
	});



	return Backbone.Marionette.CompositeView.extend({
		_msvName : 'FormInputItemList',
		template : require('hbs!tmpl/policies/GroupPermList'),
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
			return FormInputItem;
		},
		itemViewContainer : ".js-formInput",
		itemViewOptions : function() {
			return {
				'collection' 	: this.collection,
				'groupList' 	: this.groupList,
				'policyType'	: this.policyType,
				'accessTypes'	: this.accessTypes
			};
		},
		events : {
			'click [data-action="addGroup"]' : 'addNew'
		},
		initialize : function(options) {
			_.extend(this, _.pick(options, 'groupList','policyType','accessTypes','rangerServiceDefModel'));
			//this.hiveGroupPerm = _.has(options,'hiveGroupPerm') ? true : false;
			this.listenTo(this.groupList, 'sync', this.render, this);
			if(this.collection.length == 0)
				this.collection.add(new Backbone.Model());
		},
		onRender : function(){
			//console.log("onRender of ArtifactFormNoteList called");
			this.toggleAddButton();
		},
		addNew : function(){
			var that =this;
			if(this.groupList.length > this.collection.length){
				this.collection.add(new Backbone.Model());
				this.toggleAddButton();
			}
		},
		toggleAddButton : function(){
			var groupIds=[];
			this.collection.each(function(m){
				if(!_.isUndefined(m.get('groupId'))){
					var temp = m.get('groupId').split(',');
					groupIds.push.apply(groupIds,temp);
				}
			});
			if(groupIds.length == this.groupList.length)
				this.$('button[data-action="addGroup"]').hide();
			else
				this.$('button[data-action="addGroup"]').show();
		},
		getPermHeaders : function(){
			var permList = _.map(this.accessTypes,function(type){ return type.label});
			if(!_.isEmpty(this.rangerServiceDefModel.get('policyConditions'))){
				_.each(this.rangerServiceDefModel.get('policyConditions'), function(cond){
					if(!_.isNull(cond) && !_.isNull(cond.label)) permList.unshift(cond.label);
				});
			}
			permList.unshift(localization.tt('lbl.selectGroup'));
			permList.push("");
			return permList;
		},
	});

});