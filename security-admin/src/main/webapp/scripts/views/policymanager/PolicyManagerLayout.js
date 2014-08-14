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

	var Backbone			= require('backbone');

	var XALinks 			= require('modules/XALinks');
	var XAEnums 			= require('utils/XAEnums');
	var XAUtil				= require('utils/XAUtils');
	var SessionMgr 			= require('mgrs/SessionMgr');
	
	var PolicymanagerlayoutTmpl = require('hbs!tmpl/common/PolicyManagerLayout_tmpl');
	return Backbone.Marionette.Layout.extend(
	/** @lends PolicyManagerLayout */
	{
		_viewName : name,
		
    	template: PolicymanagerlayoutTmpl,

		templateHelpers: function(){
			var groupedCol = this.collection.groupBy('assetType');
			return {
				hdfsList : groupedCol[XAEnums.AssetType.ASSET_HDFS.value],
				hiveList : groupedCol[XAEnums.AssetType.ASSET_HIVE.value],
				hbaseList: groupedCol[XAEnums.AssetType.ASSET_HBASE.value],
				knoxList : groupedCol[XAEnums.AssetType.ASSET_KNOX.value],
				stormList: groupedCol[XAEnums.AssetType.ASSET_STORM.value],
				hdfsVal  : XAEnums.AssetType.ASSET_HDFS.value,
				hiveVal  : XAEnums.AssetType.ASSET_HIVE.value,
				hbaseVal : XAEnums.AssetType.ASSET_HBASE.value,
				knoxVal  : XAEnums.AssetType.ASSET_KNOX.value,
				stormVal  : XAEnums.AssetType.ASSET_STORM.value,
				assetCreateHref : XALinks.get('AssetCreate').href,
				isSysAdmin : SessionMgr.isSystemAdmin()
			};
		},
    	breadCrumbs :[XALinks.get('RepositoryManager')],

		/** Layout sub regions */
    	regions: {},

    	/** ui selector cache */
    	ui: {},

		/** ui events hash */
		events : function(){
			var events = {};
			events['click .deleteRep']		= 'onDeleteRepository';
			return events;
		},
    	/**
		* intialize a new PolicyManagerLayout Layout 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a PolicyManagerLayout Layout");

			_.extend(this, _.pick(options, 'collection'));
			this.bindEvents();
		},

		/** all events binding here */
		bindEvents : function(){
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
			this.listenTo(this.collection, "sync", this.render, this);
			this.listenTo(this.collection, "request", function(){
				this.$('[data-id="r_tableSpinner"]').removeClass('display-none').addClass('loading');
			}, this);
		},

		/** on render callback */
		onRender: function() {
			this.$('[data-id="r_tableSpinner"]').removeClass('loading').addClass('display-none');
			this.initializePlugins();
		},

		/** all post render plugin initialization */
		initializePlugins: function(){
		},
		onDeleteRepository : function(e){
			var that = this;
			var model = this.collection.get($(e.currentTarget).data('id'));
			if(model){
				model = new this.collection.model(model.attributes);
				XAUtil.confirmPopup({
					msg :'Are you sure want to delete ?',
					callback : function(){
						XAUtil.blockUI();
						model.destroy({success: function(model, response) {
							XAUtil.blockUI('unblock');
							that.collection.remove(model.get('id'));
							XAUtil.notifySuccess('Success', 'Repository deleted successfully');
							that.render();
						}});
					}
				});
			}
		},
		/** on close */
		onClose: function(){
		}

	});
});
