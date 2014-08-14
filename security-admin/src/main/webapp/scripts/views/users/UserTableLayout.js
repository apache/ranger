/*
 * Copyright (c) 2014 XASecure
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure.
 */

define(function(require){
    'use strict';

	var Backbone		= require('backbone');
	var XAEnums 		= require('utils/XAEnums');
	var XALinks 		= require('modules/XALinks');
	var XAUtil			= require('utils/XAUtils');
	var XABackgrid		= require('views/common/XABackgrid');
	var localization	= require('utils/XALangSupport');

	var VXGroupList		= require('collections/VXGroupList');
	var VXGroup			= require('models/VXGroup');
	var VXUserList		= require('collections/VXUserList');
	var XATableLayout	= require('views/common/XATableLayout');
	var vUserInfo		= require('views/users/UserInfo');

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
    		visualSearch: '.visual_search'
    	},

		/** ui events hash */
		events: function() {
			var events = {};
			events['click '+this.ui.tab+' li a']  = 'onTabChange';
			//events['change ' + this.ui.input]  = 'onInputChange';
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
			this.bindEvents();
		},

		/** all events binding here */
		bindEvents : function(){
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
		},

		/** on render callback */
		onRender: function() {
			this.initializePlugins();
			if(this.tab == 'grouptab')
				this.renderGroupTab();
			else
				this.renderUserTab();
			this.addVisualSearch();
		},
		onTabChange : function(e){
			this.showUsers = $(e.currentTarget).attr('href') == '#users' ? true : false;
			if(this.showUsers){
				this.renderUserTab();
				this.addVisualSearch();
			}
			else{
				this.renderGroupTab();
				this.addVisualSearch();
			}
			$(this.rUserDetail.el).hide();
		},
		renderUserTab : function(){
			var that = this;
			if(_.isUndefined(this.collection)){
				this.collection = new VXUserList();
			}	
			this.renderUserListTable();
			this.collection.fetch({
				cache:true
			}).done(function(){
				if(!_.isString(that.ui.addNewGroup)){
					that.ui.addNewGroup.hide();
					that.ui.addNewUser.show();
				}
				that.$('.wrap-header').text('User List');
			});
		},
		renderGroupTab : function(){
			var that = this;
			if(_.isUndefined(this.groupList)){
				this.groupList = new VXGroupList();
			}
			this.renderGroupListTable();
			this.groupList.fetch({
				cache:true
			}).done(function(){
				that.ui.addNewUser.hide();
				that.ui.addNewGroup.show();
				that.$('.wrap-header').text('Group List');
				that.$('ul').find('[data-js="groups"]').addClass('active');
				that.$('ul').find('[data-js="users"]').removeClass();
			});
		},
		renderUserListTable : function(){
			var that = this;
			var tableRow = Backgrid.Row.extend({
				events: {
					'click' : 'onClick'
				},
				initialize : function(){
					var that = this;
					var args = Array.prototype.slice.apply(arguments);
					Backgrid.Row.prototype.initialize.apply(this, args);
					this.listenTo(this.model, 'model:highlightBackgridRow', function(){
						that.$el.addClass("alert");
						$("html, body").animate({scrollTop: that.$el.position().top},"linear");
						setTimeout(function () {
							that.$el.removeClass("alert");
						}, 1500);
					}, this);
				},
				onClick: function (e) {
				   if($(e.target).is('a'))
					   return;
				   this.$el.parent('tbody').find('tr').removeClass('tr-active');
				   this.$el.toggleClass('tr-active');
				   var groupList = new VXGroupList();
				   var userModel = this.model;
				   groupList.getGroupsForUser(this.model.id,{
					  cache : false, 
					  success :function(msResponse){
						  var groupColl = msResponse.vXGroups ? msResponse.vXGroups : null; 
						  if(that.rUserDetail){
								$(that.rUserDetail.el).hide();
								$(that.rUserDetail.el).html(new vUserInfo({
									  model : userModel,
									  groupList : new VXGroupList(groupColl)
								  }).render().$el).slideDown();
							}
					  },
					  error : function(){
						  console.log('error..');
					  }
				   });
				}
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
									return '<span class="label label-important">'+XAEnums.UserTypes.USER_EXTERNAL.label+'</span>';
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
						fromRaw: function (rawValue) {
							var html = '';
							if(!_.isUndefined(rawValue)){
								_.each(rawValue,function(name){
									html += '<span class="label label-info">'+name+'</span>';
								});
								return html;
							}else
								return '--';
						}
					}),
					editable : false,
					sortable : false
				},
			/*	status : {
					label : localization.tt("lbl.status"),
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							var status = model.has('status') ? 'Active' : 'Deactive';
							if(model.has('status'))
								return '<span class="label label-success">' +status + '</span>';
							else
								return '<span class="label label-important">' +status + '</span>';
						//	return model.has('status') ? 'On' : 'Off';
						}
					}),
					click : false,
					drag : false,
					editable:false,
					sortable:false,
				},*/
				
			};
			return this.collection.constructor.getTableCols(cols, this.collection);
		},
		
		renderGroupListTable : function(){
			var that = this;
			var tableRow = Backgrid.Row.extend({
				events: {
					'click' : 'onClick'
				},
				initialize : function(){
					var that = this;
					var args = Array.prototype.slice.apply(arguments);
					Backgrid.Row.prototype.initialize.apply(this, args);
					this.listenTo(this.model, 'model:highlightBackgridRow1', function(){
						that.$el.addClass("alert");
						$("html, body").animate({scrollTop: that.$el.position().top},"linear");
						setTimeout(function () {
							that.$el.removeClass("alert");
						}, 1500);
					}, this);
				},
				onClick: function (e) {
				   if($(e.target).is('a'))
					   return;
				   this.$el.parent('tbody').find('tr').removeClass('tr-active');
				   this.$el.toggleClass('tr-active');	
				   var userList = new VXUserList();
				   var gid = this.model.id;
				   var groupModel = new VXGroup({id:gid});
				   groupModel.fetch().done(function(msResponse){
					   userList.getUsersOfGroup(gid,{
						   success :function(msResponse){
							   var userColl = msResponse.vXUsers ? msResponse.vXUsers : null; 
							   if(that.rUserDetail){
								   $(that.rUserDetail.el).hide();
								   $(that.rUserDetail.el).html(new vUserInfo({
									   model 	: groupModel,
									   userList: new VXUserList(userColl)
								   }).render().$el).slideDown(); 
							   }
						   },
						   error : function(){
							   console.log('error..');
						   }
					   });
				   });
				}
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
								else
									return '<span class="label label-important">'+XAEnums.GroupTypes.GROUP_EXTERNAL.label+'</span>';
							}else
								return '--';
						}
					}),
					click : false,
					drag : false,
					editable:false,
					sortable:false,
				}
				/*status : {
					label : localization.tt("lbl.status"),
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							var status = model.has('status') ? 'Active' : 'Deactive';
							if(model.has('status'))
								return '<span class="label label-success">' +status + '</span>';
							else
								return '<span class="label label-important">' +status + '</span>';
						}
					}),
					click : false,
					drag : false,
					editable:false,
					sortable:false,
				}, */
			};
			return this.groupList.constructor.getTableCols(cols, this.groupList);
		},
		addVisualSearch : function(){
			var coll,placeholder;
			var searchOpt = [], serverAttrName = [];
			if(this.showUsers){
				placeholder = localization.tt('h.searchForYourUser');	
				coll = this.collection;
				searchOpt = ['User Name','Email Address','Role','User Source'];//,'Start Date','End Date','Today'];
				var userRoleList = _.map(XAEnums.UserRoles,function(obj,key){return {label:obj.label,value:key};});
				serverAttrName  = [{text : "User Name", label :"name"},{text : "Email Address", label :"emailAddress"},
				                   {text : "Role", label :"userRoleList", 'multiple' : true, 'optionsArr' : userRoleList},
				                   {text : "User Source", label :"userSource", 'multiple' : true, 'optionsArr' : XAUtil.enumToSelectLabelValuePairs(XAEnums.UserTypes)},
								];
				                  // {text : 'Start Date',label :'startDate'},{text : 'End Date',label :'endDate'},
				                  // {text : 'Today',label :'today'}];
			}else{
				placeholder = localization.tt('h.searchForYourGroup');
				coll = this.groupList;
				searchOpt = ['Group Name','Group Source'];//,'Start Date','End Date','Today'];
				serverAttrName  = [{text : "Group Name", label :"name"},
				                   {text : "Group Source", label :"groupSource", 'multiple' : true, 'optionsArr' : XAUtil.enumToSelectLabelValuePairs(XAEnums.GroupTypes)},];
				
				                   //{text : 'Start Date',label :'startDate'},{text : 'End Date',label :'endDate'},
				                   //{text : 'Today',label :'today'}];

			}
			var pluginAttr = {
				      placeholder :placeholder,
				      container : this.ui.visualSearch,
				      query     : '',
				      callbacks :  { 
				    	  valueMatches :function(facet, searchTerm, callback) {
								switch (facet) {
									case 'Role':
										callback(XAUtil.hackForVSLabelValuePairs(XAEnums.UserRoles));
										break;
									case 'User Source':
										callback(XAUtil.hackForVSLabelValuePairs(XAEnums.UserTypes));
										break;	
									case 'Group Source':
										callback(XAUtil.hackForVSLabelValuePairs(XAEnums.GroupTypes));
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

		/** all post render plugin initialization */
		initializePlugins: function(){
		},

		/** on close */
		onClose: function(){
		}

	});

	return UserTableLayout; 
});
