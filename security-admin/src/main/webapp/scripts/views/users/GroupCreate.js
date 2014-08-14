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
	var App				= require('App');
	var XALinks 		= require('modules/XALinks');
	var XAUtil			= require('utils/XAUtils');
	var XAEnums			= require('utils/XAEnums');
	var localization	= require('utils/XALangSupport');
	
	var UserTableLayout	= require('views/users/UserTableLayout');
	var VXGroupList		= require('collections/VXGroupList');
	var GroupForm		= require('views/users/GroupForm');
	var GroupcreateTmpl = require('hbs!tmpl/users/GroupCreate_tmpl');

	var GroupCreate = Backbone.Marionette.Layout.extend(
	/** @lends GroupCreate */
	{
		_viewName : 'GroupCreate',
		
    	template: GroupcreateTmpl,
    	breadCrumbs :function(){
    		if(this.model.isNew())
    			return [XALinks.get('Groups'),XALinks.get('GroupCreate')];
    		else
    			return [XALinks.get('Groups'),XALinks.get('GroupEdit')];
    	},
        
		/** Layout sub regions */
    	regions: {
    		'rForm' :'div[data-id="r_form"]'
    	},

    	/** ui selector cache */
    	ui: {
    		'btnSave'	: '[data-id="save"]',
    		'btnCancel' : '[data-id="cancel"]'
    	},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			events['click ' + this.ui.btnSave]		= 'onSave';
			events['click ' + this.ui.btnCancel]	= 'onCancel';
			return events;
		},

    	/**
		* intialize a new GroupCreate Layout 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a GroupCreate Layout");

			_.extend(this, _.pick(options, ''));
			this.editGroup = this.model.has('id') ? true : false;
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
			this.form = new GroupForm({
				template : require('hbs!tmpl/users/GroupForm_tmpl'),
				model : this.model
			});
			this.rForm.show(this.form);
			this.rForm.$el.dirtyFields();
			XAUtil.preventNavigation(localization.tt('dialogMsg.preventNavGroupForm'),this.rForm.$el);
			if(!_.isUndefined(this.model.get('groupSource')) && this.model.get('groupSource') == XAEnums.GroupSource.XA_GROUP.value){
				this.ui.btnSave.hide();
			}
		},

		/** all post render plugin initialization */
		initializePlugins: function(){
		},
		onSave: function(){
			var that =this ;
			var errors = this.form.commit({validate : false});
			if(! _.isEmpty(errors)){
				return;
			}
			XAUtil.blockUI();
			
			this.model.save({},{
				success: function () {
					XAUtil.blockUI('unblock');
					XAUtil.allowNavigation();
					var msg = that.editGroup ? 'Group updated successfully' :'Group created successfully';
					XAUtil.notifySuccess('Success', msg);
					if(that.editGroup){
						var VXResourceList 	= require('collections/VXResourceList');
						var resourceList = new VXResourceList();
						_.each(Backbone.fetchCache._cache, function(url, val){
							var urlStr= resourceList.url;
							if((val.indexOf(urlStr) != -1)) 
								Backbone.fetchCache.clearItem(val);
						});
						App.appRouter.navigate("#!/users/grouptab",{trigger: true});
						return;
					}
					App.appRouter.navigate("#!/users/grouptab",{trigger: true});
					
					var groupList = new VXGroupList();
					   
					groupList.fetch({
						   cache:false
					   }).done(function(){
							var newColl;// = groupList;
							groupList.getLastPage({
								cache : false,
								success : function(collection, response, options){
									App.rContent.show(new UserTableLayout({
										groupList : collection,
										tab : 'grouptab'
									}));
									newColl = collection;
								}
							}).done(function(){
								var model = newColl.get(that.model.id);
								if(model){
									model.trigger("model:highlightBackgridRow1");
								}
							});
					   });
				},
				error : function (model, response, options) {
					XAUtil.blockUI('unblock');
					if ( response && response.responseJSON && response.responseJSON.msgDesc){
						if(response.responseJSON.msgDesc == "XGroup already exists")
							XAUtil.notifyError('Error', "Group name already exists");
						else
							XAUtil.notifyError('Error', response.responseJSON.msgDesc);
					}else
						XAUtil.notifyError('Error', 'Error creating Policy!');
					console.log('error');
				}
			});
		},
		onCancel : function(){
			XAUtil.allowNavigation();
			App.appRouter.navigate("#!/users/grouptab",{trigger: true});
		},

		/** on close */
		onClose: function(){
		}

	});

	return GroupCreate; 
});
