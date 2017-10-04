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
	var XAEnums 		= require('utils/XAEnums');
	var XALinks 		= require('modules/XALinks');
	var XAUtil			= require('utils/XAUtils');
	var XABackgrid		= require('views/common/XABackgrid');
	var localization	= require('utils/XALangSupport');
	var SessionMgr  	= require('mgrs/SessionMgr');

	var VXGroupList		= require('collections/VXGroupList');
	var VXGroup			= require('models/VXGroup');
	var VXUserList		= require('collections/VXUserList');
	var XATableLayout	= require('views/common/XATableLayout');
	var vUserInfo		= require('views/users/UserInfo');

	var VXUser          = require('models/VXUser');

	var UsertablelayoutTmpl = require('hbs!tmpl/users/UserTableLayout_tmpl');

	var UserTableLayout = Backbone.Marionette.Layout.extend(
	/** @lends UserTableLayout */
	{
		_viewName : 'UserTableLayout',
		
    	template: UsertablelayoutTmpl,
    	breadCrumbs :[XALinks.get('Users')],
		/** Layout sub regions */
    	regions: {
    		'rTableList' :'div[data-id="r_tableList"]',
    		'rUserDetail'	: '#userDetail'
    	},

    	/** ui selector cache */
    	ui: {
    		tab 		: '.nav-tabs',
    		addNewUser	: '[data-id="addNewUser"]',
    		addNewGroup	: '[data-id="addNewGroup"]',
    		visualSearch: '.visual_search',
    		btnShowMore : '[data-id="showMore"]',
			btnShowLess : '[data-id="showLess"]',
    		btnSave		: '[data-id="save"]',
    		btnShowHide		: '[data-action="showHide"]',
			visibilityDropdown		: '[data-id="visibilityDropdown"]',
			addNewBtnDiv	: '[data-id="addNewBtnDiv"]',
			deleteUser: '[data-id="deleteUserGroup"]'
    	},

		/** ui events hash */
		events: function() {
			var events = {};
			events['click '+this.ui.tab+' li a']  = 'onTabChange';
			events['click ' + this.ui.btnShowMore]  = 'onShowMore';
			events['click ' + this.ui.btnShowLess]  = 'onShowLess';
			events['click ' + this.ui.btnSave]  = 'onSave';
			events['click ' + this.ui.visibilityDropdown +' li a']  = 'onVisibilityChange';
			events['click ' + this.ui.deleteUser] = 'onDeleteUser';
			return events;
		},

    	/**
		* intialize a new UserTableLayout Layout 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a UserTableLayout Layout");

			_.extend(this, _.pick(options, 'groupList','tab'));
			this.showUsers = this.tab == 'usertab' ? true : false;
			this.chgFlags = [];
			if(_.isUndefined(this.groupList)){
				this.groupList = new VXGroupList();
			}

			this.bindEvents();
		},

		/** all events binding here */
		bindEvents : function(){
			var that = this;
			
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
		},

		/** on render callback */
		onRender: function() {
			this.initializePlugins();
			if(this.tab == 'grouptab'){
				this.renderGroupTab();
			} else {
				this.renderUserTab();
			}
			this.bindEventToColl(this.collection);
			this.bindEventToColl(this.groupList);
			this.addVisualSearch();
		},
		bindEventToColl : function(coll){
			if(_.isUndefined(coll)) return;
			this.listenTo(coll, "sync reset", function(){
				this.$el.find('table input[type="checkbox"]').prop('checked',false)
				coll.each(function(model, key, value){
					coll.trigger("backgrid:selected", model, false, true);
					coll.deselect(model);
				}, this);
			}, this);
		},
		onTabChange : function(e){
			var that = this;
			this.chgFlags = [];
			this.showUsers = $(e.currentTarget).attr('href') == '#users' ? true : false;
			if(this.showUsers){				
				this.renderUserTab();
				this.addVisualSearch();
			} else {				
				this.renderGroupTab();
				this.addVisualSearch();
			}
			$(this.rUserDetail.el).hide();
		},
		onVisibilityChange : function(e){
			var that = this;
			var status = $(e.currentTarget).attr('data-id') == 'visible' ? true : false;
			var updateReq = {};
			var collection = this.showUsers ? this.collection : this.groupList;

			collection.each(function(m){
				if(m.selected && m.get('isVisible') != status){
				  	m.set('isVisible', status);
					m.toServer();
					updateReq[m.get('id')] = m.get('isVisible');
				}
			});
			if(_.isEmpty(updateReq)){
				if(this.showUsers){
					XAUtil.alertBoxWithTimeSet(localization.tt('msg.plsSelectUserToSetVisibility'));
				}else{
					XAUtil.alertBoxWithTimeSet(localization.tt('msg.plsSelectGroupToSetVisibility'));
				}
				
				return;
			}
			var clearCache = function(coll){
					_.each(Backbone.fetchCache._cache, function(url, val){
		                   var urlStr = coll.url;
		                   if((val.indexOf(urlStr) != -1)){
		                       Backbone.fetchCache.clearItem(val);
		                   }
		                });
					coll.fetch({reset: true, cache:false}).done(function(coll){
	                	coll.each(function(model){
	                		coll.trigger("backgrid:selected", model, false, true);
	    					coll.deselect(model);
	    				}, this);
	                })
			}
			if(this.showUsers){
				collection.setUsersVisibility(updateReq, {
					success : function(){
						that.chgFlags = [];
						clearCache(collection);
					},
					error : function(resp){
						if(!_.isUndefined(resp) && !_.isUndefined(resp.responseJSON) && !_.isUndefined(resp.responseJSON.msgDesc)){
							XAUtil.notifyError('Error', resp.responseJSON.msgDesc);
						}else{
							XAUtil.notifyError('Error', "Error occurred while updating user");
						}
						collection.trigger('error','',resp)
					},
				});
			} else {
			    collection.setGroupsVisibility(updateReq, {
					success : function(){
						that.chgFlags = [];
						clearCache(collection);
					},
					error : function(resp){
						if(!_.isUndefined(resp) && !_.isUndefined(resp.responseJSON) && !_.isUndefined(resp.responseJSON.msgDesc)){
							XAUtil.notifyError('Error', resp.responseJSON.msgDesc);
						}else{
							XAUtil.notifyError('Error', "Error occurred while updating group");
						}
						collection.trigger('error','',resp)
					},
                });

			}
		},
		renderUserTab : function(){
			var that = this;
			if(_.isUndefined(this.collection)){
				this.collection = new VXUserList();
			}	
			this.collection.selectNone();
			this.renderUserListTable();
			_.extend(this.collection.queryParams, XAUtil.getUserDataParams())
			this.collection.fetch({
				//cache:true,
				reset: true,
				cache: false
//				data : XAUtil.getUserDataParams(),
			}).done(function(){
				if(!_.isString(that.ui.addNewGroup)){
					that.ui.addNewGroup.hide();
					that.ui.addNewUser.show();
				}
				that.$('.wrap-header').text('User List');
				that.checkRoleKeyAdmin();
			});
		},
		renderGroupTab : function(){
			var that = this;
			if(_.isUndefined(this.groupList)){
				this.groupList = new VXGroupList();
			}
			this.groupList.selectNone();
			this.renderGroupListTable();
			this.groupList.fetch({
				//cache:true,
				reset:true,
				cache: false
			}).done(function(){
				that.ui.addNewUser.hide();
				that.ui.addNewGroup.show();
				that.$('.wrap-header').text('Group List');
				that.$('ul').find('[data-js="groups"]').addClass('active');
				that.$('ul').find('[data-js="users"]').removeClass();
				that.checkRoleKeyAdmin();
			});
		},
		renderUserListTable : function(){
			var that = this;
			var tableRow = Backgrid.Row.extend({
				render: function () {
					tableRow.__super__.render.apply(this, arguments);
    				if(!this.model.get('isVisible')){
    					this.$el.addClass('tr-inactive');
    				}
    				return this;
				},
			});
			this.rTableList.show(new XATableLayout({
				columns: this.getColumns(),
				collection: this.collection,
				includeFilter : false,
				gridOpts : {
					row: tableRow,
					header : XABackgrid,
					emptyText : 'No Users found!'
				}
			}));	

		},

		getColumns : function(){
			var cols = {
				
				select : {
					label : localization.tt("lbl.isVisible"),
					//cell : Backgrid.SelectCell.extend({className: 'cellWidth-1'}),
					cell: "select-row",
				    headerCell: "select-all",
					click : false,
					drag : false,
					editable : false,
					sortable : false
				},
				name : {
					label	: localization.tt("lbl.userName"),
					href: function(model){
						return '#!/user/'+ model.id;
					},
					editable:false,
					sortable:false,
					cell :'uri'						
				},
				emailAddress : {
					label	: localization.tt("lbl.emailAddress"),
					cell : 'string',
					editable:false,
					sortable:false,
					placeholder : localization.tt("lbl.emailAddress")+'..'
				},
				userRoleList :{
					label	: localization.tt("lbl.role"),
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							if(!_.isUndefined(rawValue) && rawValue.length > 0){
								var role = rawValue[0];
								return '<span class="label label-info">'+XAEnums.UserRoles[role].label+'</span>';
							}
							return '--';
						}
					}),
					click : false,
					drag : false,
					editable:false,
					sortable:false,
				},
				userSource :{
					label	: localization.tt("lbl.userSource"),
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							if(!_.isUndefined(rawValue)){
								if(rawValue == XAEnums.UserSource.XA_PORTAL_USER.value)
									return '<span class="label label-success">'+XAEnums.UserTypes.USER_INTERNAL.label+'</span>';
								else
									return '<span class="label label-green">'+XAEnums.UserTypes.USER_EXTERNAL.label+'</span>';
							}else
								return '--';
						}
					}),
					click : false,
					drag : false,
					editable:false,
					sortable:false,
				},
				groupNameList : {
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					label : localization.tt("lbl.groups"),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue,model) {
							if(!_.isUndefined(rawValue)){
								return XAUtil.showGroups(_.map(rawValue,function(name){return {'userId': model.id,'groupName': name}}));
							}
							else
							return '--';
						}
					}),
					editable : false,
					sortable : false
				},
				isVisible : {
					label	: localization.tt("lbl.visibility"),
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							if(!_.isUndefined(rawValue)){
								if(rawValue)
									return '<span class="label label-success">'+XAEnums.VisibilityStatus.STATUS_VISIBLE.label+'</span>';
								else
									return '<span class="label label-green">'+XAEnums.VisibilityStatus.STATUS_HIDDEN.label+'</span>';
							}else
								return '--';
						}
					}),
					editable:false,
					sortable:false
				}
			};
			return this.collection.constructor.getTableCols(cols, this.collection);
		},
		
		renderGroupListTable : function(){
			var that = this;
			
			var tableRow = Backgrid.Row.extend({
				render: function () {
    				tableRow.__super__.render.apply(this, arguments);
    				if(!this.model.get('isVisible')){
    					this.$el.addClass('tr-inactive');
    				}
    				return this;
				},
				
			});
			this.rTableList.show(new XATableLayout({
				columns: this.getGroupColumns(),
				collection: this.groupList,
				includeFilter : false,
				gridOpts : {
					row: tableRow,
					header : XABackgrid,
					emptyText : 'No Groups found!'
				}
			}));	

		},

		getGroupColumns : function(){
			var cols = {
				
                select : {
					label : localization.tt("lbl.isVisible"),
					cell: "select-row",
				    headerCell: "select-all",
					click : false,
					drag : false,
					editable : false,
					sortable : false
				},
				name : {
					label	: localization.tt("lbl.groupName"),
					href: function(model){
						return '#!/group/'+ model.id;
					},
					editable:false,
					sortable:false,
					cell :'uri'
						
				},
				groupSource :{
					label	: localization.tt("lbl.groupSource"),
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							if(!_.isUndefined(rawValue)){
								if(rawValue == XAEnums.GroupSource.XA_PORTAL_GROUP.value)
									return '<span class="label label-success">'+XAEnums.GroupTypes.GROUP_INTERNAL.label+'</span>';
								else {
									return '<span class="label label-green">'+XAEnums.GroupTypes.GROUP_EXTERNAL.label+'</span>';
								}
							}else {
								return '--';
							}
						}
					}),
					click : false,
					drag : false,
					editable:false,
					sortable:false,
				},
				isVisible : {
					label	: localization.tt("lbl.visibility"),
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							if(!_.isUndefined(rawValue)){
								if(rawValue){
									return '<span class="label label-success">'+XAEnums.VisibilityStatus.STATUS_VISIBLE.label+'</span>';
								} else {
									return '<span class="label label-green">'+XAEnums.VisibilityStatus.STATUS_HIDDEN.label+'</span>';
								}
							}else {
								return '--';
							}
						}
					}),
					editable:false,
					sortable:false
				}
			};
			return this.groupList.constructor.getTableCols(cols, this.groupList);
		},

		onUserGroupDeleteSuccess: function(jsonUsers,collection){
			_.each(jsonUsers.vXStrings,function(ob){
				var model = _.find(collection.models, function(mo){
					if(mo.get('name') === ob.value)
						return mo;
					});
				collection.remove(model.get('id'));
			});
		},

		onDeleteUser: function(e){

			var that = this;
			var collection = that.showUsers ? that.collection : that.groupList;
			var selArr = [];
			var message = '';
			collection.each(function(obj){
				if(obj.selected){
	                selArr.push({"value" : obj.get('name') , "id" : obj.get('id')});
	            }
            });
			var  vXStrings = [];
			var jsonUsers  = {};
			for(var i in selArr) {
				var itemName = selArr[i].value , itemId = selArr[i].id;
				vXStrings.push({
					"value" : itemName,
					"id" : itemId
				});
			}
			jsonUsers.vXStrings = vXStrings;

			var total_selected = jsonUsers.vXStrings.length;
			if(total_selected == 0){
				if(that.showUsers){
					XAUtil.alertBoxWithTimeSet(localization.tt('msg.noDeleteUserRow'));
				}else{
					XAUtil.alertBoxWithTimeSet(localization.tt('msg.noDeleteGroupRow'));
				}
				return;
			}
            if(total_selected == 1) {
                message = 'Are you sure you want to delete '+(that.showUsers ? 'user':'group')+' \''+ _.escape( jsonUsers.vXStrings[0].value )+'\'?';
			}
			else {
				message = 'Are you sure you want to delete '+total_selected+' '+(that.showUsers ? 'users':'groups')+'?';
			}
			if(total_selected > 0){
				XAUtil.confirmPopup({
					msg: message,
					callback: function(){
						XAUtil.blockUI();
						if(that.showUsers){
							var model = new VXUser();
                            var count = 0 , notDeletedUserName = "";
                            _.map(jsonUsers.vXStrings , function(m){
                            	model.deleteUsers(m.id,{
                            		success: function(response,options){
                            			count += 1;
                            			that.userCollection(jsonUsers.vXStrings.length, count, notDeletedUserName)
                            		},
                            		error:function(response,options){
                            			count += 1;
                            			notDeletedUserName += m.value + ", ";
                            			that.userCollection(jsonUsers.vXStrings.length, count, notDeletedUserName)
                            		}
                            	});
                            });
                        }else {
							var model = new VXGroup();
                            var count = 0, notDeletedGroupName ="";
                            _.map(jsonUsers.vXStrings, function(m){
                            	model.deleteGroups(m.id,{
                            		success: function(response){
                            			count += 1;
                            			that.groupCollection(jsonUsers.vXStrings.length,count,notDeletedGroupName)
                            		},
                            		error:function(response,options){
                            			count += 1;
                            			notDeletedGroupName += m.value + ", ";
                            			that.groupCollection(jsonUsers.vXStrings.length,count, notDeletedGroupName)
                            		}
                            	})
                            });
						}
					}
				});
			}
		},
        userCollection : function(numberOfUser, count, notDeletedUserName){
                if(count == numberOfUser){
                        this.collection.getFirstPage({fetch:true});
                        this.collection.selected = {};
                        XAUtil.blockUI('unblock');
                        if(notDeletedUserName === ""){
                                XAUtil.notifySuccess('Success','User deleted successfully!');
                        }else{
                                XAUtil.notifyError('Error', 'Error occurred during deleting Users: '+ notDeletedUserName.slice(0 , -2));
                        }
                }
        },
        groupCollection : function(numberOfGroup, count ,notDeletedGroupName){
                if(count == numberOfGroup){
                        this.groupList.getFirstPage({fetch:true});
                        this.groupList.selected  = {};
                        XAUtil.blockUI('unblock');
                        if(notDeletedGroupName === ""){
                                XAUtil.notifySuccess('Success','Group deleted successfully!');
                        } else {
                                XAUtil.notifyError('Error', 'Error occurred during deleting Groups: '+ notDeletedGroupName.slice(0 , -2));
                        }
                }
        },
		addVisualSearch : function(){
			var that = this;
			var coll,placeholder;
			var searchOpt = [], serverAttrName = [];
			if(this.showUsers){
				placeholder = localization.tt('h.searchForYourUser');	
				coll = this.collection;
				searchOpt = ['User Name','Email Address','Visibility', 'Role','User Source','User Status'];//,'Start Date','End Date','Today'];
				var userRoleList = _.map(XAEnums.UserRoles,function(obj,key){return {label:obj.label,value:key};});
				serverAttrName  = [	{text : "User Name", label :"name"},
									{text : "Email Address", label :"emailAddress"},
				                   {text : "Role", label :"userRole", 'multiple' : true, 'optionsArr' : userRoleList},
				                   	{text : "Visibility", label :"isVisible", 'multiple' : true, 'optionsArr' : XAUtil.enumToSelectLabelValuePairs(XAEnums.VisibilityStatus)},
				                   {text : "User Source", label :"userSource", 'multiple' : true, 'optionsArr' : XAUtil.enumToSelectLabelValuePairs(XAEnums.UserTypes)},
				                   {text : "User Status", label :"status", 'multiple' : true, 'optionsArr' : XAUtil.enumToSelectLabelValuePairs(XAEnums.ActiveStatus)},
								];
			} else {
				placeholder = localization.tt('h.searchForYourGroup');
				coll = this.groupList;
				searchOpt = ['Group Name','Group Source', 'Visibility'];//,'Start Date','End Date','Today'];
				serverAttrName  = [{text : "Group Name", label :"name"},
				                   {text : "Visibility", label :"isVisible", 'multiple' : true, 'optionsArr' : XAUtil.enumToSelectLabelValuePairs(XAEnums.VisibilityStatus)},
				                   {text : "Group Source", label :"groupSource", 'multiple' : true, 'optionsArr' : XAUtil.enumToSelectLabelValuePairs(XAEnums.GroupTypes)},];

			}
			var query = (!_.isUndefined(coll.VSQuery)) ? coll.VSQuery : '';
			var pluginAttr = {
				      placeholder :placeholder,
				      container : this.ui.visualSearch,
				      query     : query,
				      callbacks :  { 
				    	  valueMatches :function(facet, searchTerm, callback) {
								switch (facet) {
									case 'Role':
										var roles = XAUtil.hackForVSLabelValuePairs(XAEnums.UserRoles);
										var label  = SessionMgr.isSystemAdmin() || SessionMgr.isUser() ? XAEnums.UserRoles.ROLE_KEY_ADMIN.label
													: XAEnums.UserRoles.ROLE_SYS_ADMIN.label;
										callback(_.filter(roles, function(o) { return o.label !== label; }));
										break;
									case 'User Source':
										callback(XAUtil.hackForVSLabelValuePairs(XAEnums.UserTypes));
										break;	
									case 'Group Source':
										callback(XAUtil.hackForVSLabelValuePairs(XAEnums.GroupTypes));
										break;		
									case 'Visibility':
										callback(XAUtil.hackForVSLabelValuePairs(XAEnums.VisibilityStatus));
										break;
									case 'User Status':
//										callback(XAUtil.hackForVSLabelValuePairs(XAEnums.ActiveStatus));
										callback(that.getActiveStatusNVList());
										break;
									/*case 'Start Date' :
										setTimeout(function () { XAUtil.displayDatepicker(that.ui.visualSearch, callback); }, 0);
										break;
									case 'End Date' :
										setTimeout(function () { XAUtil.displayDatepicker(that.ui.visualSearch, callback); }, 0);
										break;
									case 'Today'	:
										var today = Globalize.format(new Date(),"yyyy/mm/dd");
										callback([today]);
										break;*/
								}     
			            	
							}
				      }
				};
			XAUtil.addVisualSearch(searchOpt,serverAttrName, coll,pluginAttr);
		},
		getActiveStatusNVList : function() {
			var activeStatusList = _.filter(XAEnums.ActiveStatus, function(obj){
				if(obj.label != XAEnums.ActiveStatus.STATUS_DELETED.label)
					return obj;
			});
			return _.map(activeStatusList, function(status) { return { 'label': status.label, 'value': status.label}; })
		},
		onShowMore : function(e){
			var id = $(e.currentTarget).attr('policy-group-id');
			this.rTableList.$el.find('[policy-group-id="'+id+'"]').show();
			$('[data-id="showLess"][policy-group-id="'+id+'"]').show();
			$('[data-id="showMore"][policy-group-id="'+id+'"]').hide();
			$('[data-id="showMore"][policy-group-id="'+id+'"]').parents('div[data-id="groupsDiv"]').addClass('set-height-groups');
		},
		onShowLess : function(e){
			var id = $(e.currentTarget).attr('policy-group-id');
			this.rTableList.$el.find('[policy-group-id="'+id+'"]').slice(4).hide();
			$('[data-id="showLess"][policy-group-id="'+id+'"]').hide();
			$('[data-id="showMore"][policy-group-id="'+id+'"]').show();
			$('[data-id="showMore"][policy-group-id="'+id+'"]').parents('div[data-id="groupsDiv"]').removeClass('set-height-groups')
		},
		checkRoleKeyAdmin : function() {
			if(SessionMgr.isKeyAdmin()){
				this.ui.addNewBtnDiv.children().hide()
			}
		},
		/** all post render plugin initialization */
		initializePlugins: function(){
		},

		/** on close */
		onClose: function(){
			XAUtil.allowNavigation();
		}

	});

	return UserTableLayout; 
});
