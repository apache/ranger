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
	var SessionMgr 		= require('mgrs/SessionMgr');

	var VXGroup			= require('models/VXGroup');
	var VXUser				= require('models/VXUser');
	var VXGroupList			= require('collections/VXGroupList');
	var VXUserList			= require('collections/VXUserList');
	
	require('bootstrap-editable');
	require('esprima');
    	
	var PermissionItem = Backbone.Marionette.ItemView.extend({
		_msvName : 'PermissionItem',
		template : require('hbs!tmpl/policies/PermissionItem'),
		tagName : 'tr',
		templateHelpers : function(){
			
			return {
				permissions 	: this.accessTypes,
				policyConditions: this.policyConditions,
				isModelNew		: !this.model.has('editMode'),
				perms			: this.permsIds.length == 14 ? _.union(this.permsIds,[-1]) : this.permsIds,
			    isMaskingPolicy : XAUtil.isMaskingPolicy(this.rangerPolicyType),
			    isAccessPolicy 	: XAUtil.isAccessPolicy(this.rangerPolicyType),
			    isRowFilterPolicy	: XAUtil.isRowFilterPolicy(this.rangerPolicyType),
			};
		},
		ui : {
			selectGroups	: '[data-js="selectGroups"]',
			selectUsers		: '[data-js="selectUsers"]',
			addPerms		: 'a[data-js="permissions"]',
			maskingType		: 'a[data-js="maskingType"]',
			rowLeveFilter	: 'a[data-js="rowLeveFilter"]',
			conditionsTags	: '[class=tags1]',
			delegatedAdmin	: 'input[data-js="delegatedAdmin"]',
			addPermissionsSpan : '.add-permissions',
			addConditionsSpan  : '.add-conditions',
			addMaskingTypeSpan : '.add-masking-type',
			addRowFilterSpan   : '.add-row-filter',
			
		},
		events : {
			'click [data-action="delete"]'	: 'evDelete',
			'click [data-js="delegatedAdmin"]'	: 'evClickTD',
			'change [data-js="selectGroups"]': 'evSelectGroup',
			'change [data-js="selectUsers"]': 'evSelectUser',
			'change input[class="policy-conditions"]'	: 'policyCondtionChange'
		},

		initialize : function(options) {
			_.extend(this, _.pick(options, 'groupList','accessTypes','policyConditions','userList','rangerServiceDefModel','rangerPolicyType'));
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
			if(this.rangerServiceDefModel.get('name') == XAEnums.ServiceType.SERVICE_TAG.label){
				this.renderPermsForTagBasedPolicies()
			} else {
				this.renderPerms();
			}
			this.renderPolicyCondtion();
			if(XAUtil.isMaskingPolicy(this.rangerPolicyType)){
				this.renderMaskingType();
			}
			if(XAUtil.isRowFilterPolicy(this.rangerPolicyType)){
				this.renderRowLevelFilter();
			}
			
		},
		setupFormForEditMode : function() {
			var permTypes = this.accessTypes;
			
			this.accessItems = _.map(permTypes, function(perm){ 
				if(!_.isUndefined(perm)) return {'type':perm.name, isAllowed : false}
			});
			if(this.model.has('editMode') && this.model.get('editMode')){
				if(!_.isUndefined(this.model.get('groupName')) && !_.isNull(this.model.get('groupName'))){
					this.ui.selectGroups.val(_.map(this.model.get('groupName'), function(name){ return _.escape(name); }));
				}
				if(!_.isUndefined(this.model.get('userName')) && !_.isNull(this.model.get('userName'))){
					this.ui.selectUsers.val(_.map(this.model.get('userName'), function(name){ return _.escape(name); }));
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
				if(!_.isUndefined(this.model.get('rowFilterInfo')) && !_.isUndefined(this.model.get('rowFilterInfo').filterExpr)){
					this.rowFilterExprVal = this.model.get('rowFilterInfo').filterExpr
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
				var name = ($(e.currentTarget).attr('data-js') == that.ui.selectGroups.attr('data-js')) ? 'group': 'user';
				that.checkDirtyFieldForDropDown(e);
				
				if(e.removed != undefined){
					var gNameArr = [];
					if(that.model.get(name+'Name') != undefined)
						gNameArr = _.without(that.model.get(name+'Name'), e.removed.text);
					if(!_.isEmpty(gNameArr)){
						that.model.set(name+'Name',gNameArr);
					} else {
						that.model.unset(name+'Name');
					}
					return;
				}
				if(!_.isUndefined(e.added)){
					var nameList = _.map($(e.currentTarget).select2("data"), function(obj){return obj.text});
					that.model.set(name+'Name', nameList);
				}
			});
		},
		createDropDown :function($select, list, typeGroup){
			var that = this,
			placeholder = (typeGroup) ? 'Select Group' : 'Select User',
			searchUrl   = (typeGroup) ? "service/xusers/groups" : "service/xusers/users",
			getUrl 		= (typeGroup) ? "service/xusers/groups/groupName/" : "service/xusers/users/userName/";
			if(this.model.has('editMode') && !_.isEmpty($select.val())){
				var temp = $select.val().split(",");
				_.each(temp , function(name){
					if(_.isEmpty(list.where({ 'name' : name}))){
						var model = typeGroup ? new VXGroup() : new VXUser();
						model.urlRoot = getUrl + name;
						model.fetch({async:false}).done(function(){
							list.add(model);
						});
					}
				});
			}
			var tags = list.map(function(m){
//				return { id : m.id+"" , text : _.escape(m.get('name'))};
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
					var data = [], names = (typeGroup) ? that.model.get('groupName') : that.model.get('userName');
					_.each(names, function (name) {
//						name = _.escape(name);
						var obj = _.findWhere(tags, {text: name });
						data.push({ id : obj.id, text : name })
					});
					callback(data);
				},
				ajax: { 
					url: searchUrl,
					dataType: 'json',
					data: function (term, page) {
						return {name : term, isVisible : XAEnums.VisibilityStatus.STATUS_VISIBLE.value};
					},
					results: function (data, page) { 
						var results = [] , selectedVals = [];
						//Get selected values of groups/users dropdown
						selectedVals = that.getSelectedValues($select, typeGroup);
						if(data.resultSize != "0"){
							if(typeGroup){
								results = data.vXGroups.map(function(m, i){	return {id : m.id+"", text: _.escape(m.name) };	});
							} else {
								results = data.vXUsers.map(function(m, i){	return {id : m.id+"", text: _.escape(m.name) };	});
							}
							if(!_.isEmpty(selectedVals)){
								results = XAUtil.filterResultByText(results, selectedVals);
							}
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
			//set default access type 'select' for add new masking & row filter policies
			if(!XAUtil.isAccessPolicy(this.rangerPolicyType) && !_.contains(this.permsIds,'select')) {
				this.permsIds.push('select');
			}
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
		renderPermsForTagBasedPolicies :function(){
			var that = this;
			this.ui.addPerms.attr('data-type','tagchecklist')
			this.ui.addPerms.attr('title','Components Permissions')
			this.ui.delegatedAdmin.parent('td').hide();
			this.perms =  _.map(this.accessTypes,function(m){return {text:m.label, value:m.name};});

			//create x-editable for permissions
			this.ui.addPerms.editable({
			    emptytext : 'Add Permissions',
				source: this.perms,
				value : this.permsIds,
				placement : 'top',
				showbuttons : 'bottom',
				display: function(values,srcData) {
					if(_.isNull(values) || _.isEmpty(values)){
						$(this).empty();
						that.model.unset('accesses');
						that.ui.addPermissionsSpan.find('i').attr('class', 'icon-plus');
						that.ui.addPermissionsSpan.attr('title','add');
						return;
					}
					if(_.contains(values,"on")){
						values = _.without(values,"on")
					}
					//To remove selectall options
					values = _.uniq(values);
					if(values.indexOf("selectall") >= 0){
						values.splice(values.indexOf("selectall"), 1)
					}
//			    	that.checkDirtyFieldForGroup(values);
					
					var permTypeArr = [];
					var valArr = _.map(values, function(id){
						if(!_.isUndefined(id)){
							var obj = _.findWhere(srcData,{'value' : id});
							permTypeArr.push({permType : obj.value});
							return "<span class='label label-info'>" + id.substr(0,id.indexOf(":")).toUpperCase() + "</span>";
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
					$(this).html(_.uniq(valArr).join(" "));
					that.ui.addPermissionsSpan.find('i').attr('class', 'icon-pencil');
					that.ui.addPermissionsSpan.attr('title','edit');
				},
			}).on('hide',function(e){
					$(e.currentTarget).parent().find('.tag-fixed-popover-wrapper').remove()
			}).on('click', function(e) {
				e.stopPropagation();
				e.preventDefault();
				that.clickOnPermissions(that);
				 //Sticky popup
				var pop = $(this).parent('td').find('.popover')
				pop.wrap('<div class="tag-fixed-popover-wrapper"></div>');
				pop.addClass('tag-fixed-popover');
				pop.find('.arrow').removeClass('arrow')
			});
			that.ui.addPermissionsSpan.click(function(e) {
				e.stopPropagation();
				that.$('a[data-js="permissions"]').editable('toggle');
//				that.clickOnPermissions(that);
				
				var pop = $(this).parent('td').find('.popover')
				pop.wrap('<div class="tag-fixed-popover-wrapper"></div>');
				pop.addClass('tag-fixed-popover');
				pop.find('.arrow').removeClass('arrow')
			});
			
		},
		clickOnPermissions : function(that) {
			var selectAll = true;
			var checklist = that.$('.editable-checklist').find('input[type="checkbox"]')
			
			_.each(checklist,function(checkbox){ if($(checkbox).val() != -1 && !$(checkbox).is(':checked')) selectAll = false;})
			if(selectAll){
				that.$('.editable-checklist').find('input[type="checkbox"][value="-1"]').prop('checked',true)
			} else {
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
					if(!_.isUndefined(obj.evaluatorOptions) && !_.isUndefined(obj.evaluatorOptions['ui.isMultiline']) && Boolean(obj.evaluatorOptions['ui.isMultiline'])){
						return '<div class="editable-address margin-bottom-5"><label style="display:block !important;"><span>'+obj.label+' : </span><i title="JavaScript Condition Examples :\ncountry_code == \'USA\', time_range >= 900 && time_range <= 1800 etc." class="icon-info-sign" style="float: right;margin-top: 6px;"></i>\
						</label><textarea name="'+obj.name+'" placeholder="Please enter condtion.."></textarea></div>'
					}
					return '<div class="editable-address margin-bottom-5"><label style="display:block !important;"><span>'+obj.label+' : </span></label><input type="text" name="'+obj.name+'" ></div>'
						
				});
				//to show only mutiline line policy codition 
				this.multiLinecond = _.filter(that.policyConditions, function(m){ return (!_.isUndefined(m.evaluatorOptions['ui.isMultiline']) && m.evaluatorOptions['ui.isMultiline']) });
				this.multiLinecond = _.isArray(this.multiLinecond) ? this.multiLinecond : [this.multiLinecond];
				//get the select input size(for bootstrap x-editable) of policy conditions
				var selectSizeList = [];
                _.each(this.policyConditions,function(policyCondition){
                	if (XAUtil.isSinglevValueInput(policyCondition)) {
                		selectSizeList.push(1);
                	} else {
                		selectSizeList.push(undefined);
                	}
                });
              //Create new bootstrap x-editable `policyConditions` dataType for policy conditions
                XAUtil.customXEditableForPolicyCond(tmpl.join(''),selectSizeList);
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
							//Generate html to show on UI
							var html = _.map(value, function(val,name) {
								var label = (i%2 == 0) ? 'label label-inverse' : 'label';
								if(_.isEmpty(val)){
									return ''; 
								}
								//Add label for policy condition
								var pcond = _.findWhere(that.multiLinecond, { 'name': name})
								if(!_.isUndefined(pcond) && !_.isUndefined(pcond['evaluatorOptions']) 
										&& ! _.isUndefined(pcond['evaluatorOptions']["ui.isMultiline"]) 
										&& ! _.isUndefined(pcond['evaluatorOptions']['engineName'])){
									val = 	pcond['evaluatorOptions']['engineName'] + ' Condition'
								}
								i++;
								return '<span class="'+label+' white-space-normal" >'+name+' : '+ val + '</span>';
							});
							var cond = _.map(value, function(val, name) {
								return {'type' : name, 'values' : !_.isArray(val) ?  val.split(', ') : val};
							});
							
							that.model.set('conditions', cond);
							$(this).html(html);
							that.ui.addConditionsSpan.find('i').attr('class', 'icon-pencil');
							that.ui.addConditionsSpan.attr('title','edit');
						} else {
							that.model.unset('conditions');
							$(this).empty();
							that.ui.addConditionsSpan.find('i').attr('class', 'icon-plus');
							that.ui.addConditionsSpan.attr('title','add');
						}
					},
					validate:function(value){
						var error = {'flag' : false};
						_.each(value, function(val, name){
							var tmp = _.findWhere(that.multiLinecond, { 'name' : name});
							if(!_.isUndefined(tmp)){
								try {
									var t = esprima.parse(val);
								}catch(e){
									if(!error.flag){
										console.log(e.message)
										error.flag = true;
										error.message = e.message;
										error.fieldName = name;
									}
								}
							}
						})
						$('.editableform').find('.editable-error-block').remove();
						if(error.flag){
							$('.editableform').find('.editable-error-block').remove();
							$('.editableform').find('[name="'+error.fieldName+'"]').parent().append('<div class="editable-error-block help-block" style="display: none;"></div>')
							return error.message;
						}
				    },
				});
				that.ui.addConditionsSpan.click(function(e) {
					e.stopPropagation();
					that.$('#policyConditions').editable('toggle');
				});
				
			}
		},
		getSelectedValues : function($select, typeGroup){
			var vals = [],selectedVals = [];
			var name = typeGroup ? 'group' : 'user';
			if(!_.isEmpty($select.select2('data'))){
				selectedVals = _.map($select.select2('data'),function(obj){ return obj.text; });
			}
			vals.push.apply(vals , selectedVals);
			vals = $.unique(vals);
			return vals;
		},
		evDelete : function(){
			var that = this;
			this.collection.remove(this.model);
		},
		evClickTD : function(e){
			var $el = $(e.currentTarget);
			XAUtil.checkDirtyFieldForToggle($el);
			//Set Delegated Admin value 
			if(!_.isUndefined($el.find('input').data('js'))){
				this.model.set('delegateAdmin',$el.is(':checked'));
			}
			//select/deselect all functionality
			if(this.checkAll($el.find('input[type="checkbox"][value!="-1"]'))){
				$el.find('input[type="checkbox"][value="-1"]').prop('checked', true)
			} else {
			    $el.find('input[type="checkbox"][value="-1"]').prop('checked', false)
			}

		},
		checkAll : function($inputs){
			 var checkall = true;
			 $inputs.each(function(idx, input){
			    if(!checkall)   return;
			 	checkall = $(input).is(':checked') ? true : false
			 });
			 return checkall;
		},
		checkDirtyFieldForCheckBox : function(perms){
			var permList = [];
			if(!_.isUndefined(this.model.get('_vPermList')))
				permList = _.map(this.model.attributes._vPermList,function(obj){return obj.permType;});
			perms = _.map(perms,function(obj){return obj.permType;});
			XAUtil.checkDirtyField(permList, perms, this.$el);
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
		renderMaskingType :function(){
			var that = this, maskingTypes = [];
			this.maskTypeIds =  [];
			if(!_.isUndefined(this.model.get('dataMaskInfo')) && !_.isUndefined(this.model.get('dataMaskInfo').dataMaskType)){
				this.maskTypeIds = this.model.get('dataMaskInfo').dataMaskType
			}
			
			if(!_.isUndefined(this.rangerServiceDefModel.get('dataMaskDef')) && !_.isUndefined(this.rangerServiceDefModel.get('dataMaskDef').maskTypes)){
				maskingTypes = this.rangerServiceDefModel.get('dataMaskDef').maskTypes;
			}
			this.maskTypes =  _.map(maskingTypes, function(m){return {text:m.label, value : m.name };});
			//create x-editable for permissions
			this.ui.maskingType.editable({
			    emptytext : localization.tt('lbl.selectMaskingOption'),
				source: this.maskTypes,
				value : this.maskTypeIds,
				display: function(value,srcData) {
					if(_.isNull(value) || _.isEmpty(value)){
						$(this).empty();
						that.model.unset('dataMaskInfo');
						that.ui.addMaskingTypeSpan.find('i').attr('class', 'icon-plus');
						that.ui.addMaskingTypeSpan.attr('title','add');
						return;
					}
					
					var obj = _.findWhere(srcData, {'value' : value } );
					// Save form data to model
					that.model.set('dataMaskInfo', {'dataMaskType': value });
					//Custom dataMaskType
					if(value === "CUSTOM"){
						$(this).siblings('[data-id="maskTypeCustom"]').css("display","");
					}else{
						$(this).siblings('[data-id="maskTypeCustom"]').css("display","none");
					}
					
					$(this).html("<span class='label label-info'>" + obj.text + "</span>");
					that.ui.addMaskingTypeSpan.find('i').attr('class', 'icon-pencil');
					that.ui.addMaskingTypeSpan.attr('title','edit');
				},
			}).on('click', function(e) {
				e.stopPropagation();
				e.preventDefault();
//				that.clickOnMaskingType(that);
			});
			that.ui.addMaskingTypeSpan.click(function(e) {
				e.stopPropagation();
				that.$('a[data-js="maskingType"]').editable('toggle');
//				that.clickOnMaskingType(that);
			});
			this.$el.find('input[data-id="maskTypeCustom"]').on('change', function(e){
				if(!_.isUndefined(that.model.get('dataMaskInfo'))){
					that.model.get('dataMaskInfo').valueExpr = e.currentTarget.value;
				}
			});
		},
		renderRowLevelFilter :function(){
			var that = this;
			//create x-editable for permissions
			this.ui.rowLeveFilter.editable({
			    emptytext : 'Add Row Filter',
			    placeholder : 'enter expression',	
				value : this.rowFilterExprVal,
				display: function(value,srcData) {
					if(_.isNull(value) || _.isEmpty(value)){
						$(this).empty();
						that.model.unset('rowFilterInfo');
						that.ui.addRowFilterSpan.find('i').attr('class', 'icon-plus');
						that.ui.addRowFilterSpan.attr('title','add');
						return;
					}	
					that.model.set('rowFilterInfo', {'filterExpr': value });
					$(this).html("<span class='label label-info'>" + _.escape(value) + "</span>");
					that.ui.addRowFilterSpan.find('i').attr('class', 'icon-pencil');
					that.ui.addRowFilterSpan.attr('title','edit');
				},
			}).on('click', function(e) {
				e.stopPropagation();
				e.preventDefault();
			});
			that.ui.addRowFilterSpan.click(function(e) {
				e.stopPropagation();
				that.$('a[data-js="rowLeveFilter"]').editable('toggle');
			});
			
		},

	});



	return Backbone.Marionette.CompositeView.extend({
		_msvName : 'PermissionItemList',
		template : require('hbs!tmpl/policies/PermissionList'),
		templateHelpers :function(){
			return {
				permHeaders : this.getPermHeaders(),
				headerTitle : this.headerTitle
			};
		},
		getItemView : function(item){
			if(!item){
				return;
			}
			return PermissionItem;
		},
		itemViewContainer : ".js-formInput",
		itemViewOptions : function() {
			//set access type by policy type
			this.setAccessTypeByPolicyType();
			return {
				'collection' 	: this.collection,
				'groupList' 	: this.groupList,
				'userList' 	: this.userList,
				'accessTypes'	: this.accessTypes,
				'policyConditions' : this.rangerServiceDefModel.get('policyConditions'),
				'rangerServiceDefModel' : this.rangerServiceDefModel,
				'rangerPolicyType' : this.rangerPolicyType
			};
		},
		events : {
			'click [data-action="addGroup"]' : 'addNew'
		},
		initialize : function(options) {
			_.extend(this, _.pick(options, 'groupList','accessTypes','rangerServiceDefModel','userList', 'headerTitle','rangerPolicyType'));
			this.listenTo(this.groupList, 'sync', this.render, this);
			if(this.collection.length == 0)
				this.collection.add(new Backbone.Model());
		},
		onRender : function(){
			this.makePolicyItemSortable();
		},

		addNew : function(){
			var that =this;
			this.collection.add(new Backbone.Model());
		},
		getPermHeaders : function(){
			var permList = [];
			if(this.rangerServiceDefModel.get('name') != XAEnums.ServiceType.SERVICE_TAG.label){
				if(XAUtil.isAccessPolicy(this.rangerPolicyType)){
					permList.unshift(localization.tt('lbl.delegatedAdmin'));
				}
				if(XAUtil.isRowFilterPolicy(this.rangerPolicyType)){
					permList.unshift(localization.tt('lbl.rowLevelFilter'));
					permList.unshift(localization.tt('lbl.accessTypes'));
				}else if(XAUtil.isMaskingPolicy(this.rangerPolicyType)){
					permList.unshift(localization.tt('lbl.selectMaskingOption'));
					permList.unshift(localization.tt('lbl.accessTypes'));
				}else{
					permList.unshift(localization.tt('lbl.permissions'));
				}
			} else {
				permList.unshift(localization.tt('lbl.componentPermissions'));
			}
			
			if(!_.isEmpty(this.rangerServiceDefModel.get('policyConditions'))){
				permList.unshift(localization.tt('h.policyCondition'));
			}
			permList.unshift(localization.tt('lbl.selectUser'));
			permList.unshift(localization.tt('lbl.selectGroup'));
			permList.push("");
			return permList;
		},
		setAccessTypeByPolicyType : function(){
			if(XAUtil.isMaskingPolicy(this.rangerPolicyType) && XAUtil.isRenderMasking(this.rangerServiceDefModel.get('dataMaskDef'))){
				var dataMaskDef = this.rangerServiceDefModel.get('dataMaskDef');
				if(!_.isUndefined(dataMaskDef) && !_.isUndefined(dataMaskDef.accessTypes)){
					this.accessTypes =  _.map(dataMaskDef.accessTypes, function(m){return _.findWhere(this.accessTypes, {'name' : m.name });}, this);
				}
			}else if(XAUtil.isRowFilterPolicy(this.rangerPolicyType) && XAUtil.isRenderRowFilter(this.rangerServiceDefModel.get('rowFilterDef'))){
				var rowFilterDef = this.rangerServiceDefModel.get('rowFilterDef');
				if(!_.isUndefined(rowFilterDef) && !_.isUndefined(rowFilterDef.accessTypes)){
					this.accessTypes =  _.map(rowFilterDef.accessTypes, function(m){return _.findWhere(this.accessTypes, {'name' : m.name });}, this);
				}
			}
		},
		makePolicyItemSortable : function(){
			var that = this, draggedModel;
			this.$el.find(".js-formInput" ).sortable({
				placeholder: "ui-state-highlight",
				start : function(event, ui){
					var row = ui.item[0].rowIndex - 1;
					draggedModel = that.collection.at(row);
				},
				stop : function(event, ui){
					var row = ui.item[0].rowIndex -1;
					that.collection.remove(draggedModel, { silent : true});
					that.collection.add(draggedModel ,{ at: row, silent : true });
					that.$el.find(ui.item[0]).addClass("dirtyField");
				},
			});
		}
	});

});
