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
	var Communicator	= require('communicator');
	var App				= require('App');
	var XALinks 		= require('modules/XALinks');
	var XAEnums 		= require('utils/XAEnums');
	var XAGlobals 		= require('utils/XAGlobals');
	var localization	= require('utils/XALangSupport');
	
	var XABackgrid		= require('views/common/XABackgrid');
	var XATableLayout	= require('views/common/XATableLayout');

	var AssettablelayoutTmpl = require('hbs!tmpl/asset/AssetTableLayout_tmpl'); 

	require('backgrid-filter');
	require('backgrid-paginator');
	require('bootbox');

	var AssetTableLayout = Backbone.Marionette.Layout.extend(
	/** @lends AssetTableLayout */
	{
		_viewName : 'AssetTableLayout',
		
    	template: AssettablelayoutTmpl,
        
		/*
    	breadCrumbs :function(){
    		if(this.model.isNew())
    			return [XALinks.get(''), XALinks.get('')];
    		else
    			return [XALinks.get(''), XALinks.get('')];
    	},        
		*/

		/** Layout sub regions */
    	regions: {
			'rTableList'	: 'div[data-id="r_assettable"]'
		},

    	/** ui selector cache */
    	ui: {},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			return events;
		},

    	/**
		* intialize a new AssetTableLayout Layout 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a AssetTableLayout Layout");

			_.extend(this, _.pick(options, ''));
			
			this.collection.extraSearchParams = {
				//resourceType : XAEnums.AssetType.ASSET_HDFS.value	
			};

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
			this.renderTable();
		},

		/** all post render plugin initialization */
		initializePlugins: function(){
		},

		renderTable : function(){
			var that = this;
			var TableRow = Backgrid.Row.extend({
				events: {
					'click' : 'onClick'
				},
				onClick: function (e) {
					if($(e.toElement).is('.icon-edit'))
						return;
					this.$el.parent('tbody').find('tr').removeClass('tr-active');
					this.$el.toggleClass('tr-active');
					that.rFolderInfo.show(new vFolderInfo({
						model : this.model
					}));
									
				}
			});

			this.rTableList.show(new XATableLayout({
				columns: this.getColumns(),
				collection: this.collection,
				includeFilter : false,
				gridOpts : {
					row: TableRow,
					header : XABackgrid,
					emptyText : localization.tt('plcHldr.noAssets')
				},
				filterOpts : {
				  name: ['name'],
				  placeholder: localization.tt('plcHldr.searchByResourcePath'),
				  wait: 150
				}
			}));
		},

		getColumns : function(){
			var that = this;
			var cols = {
				name : {
					label	: localization.tt("lbl.resourcePath"),
					placeholder : 'Resource Path',
					editable:false
					//cell :"uri,"
					/*href: function(model){
						return '#!/policy/' + model.id;
					}*/
				}
				
			};
			return this.collection.constructor.getTableCols(cols, this.collection);
		},


		/** on close */
		onClose: function(){
		}

	});

	return AssetTableLayout; 
});
