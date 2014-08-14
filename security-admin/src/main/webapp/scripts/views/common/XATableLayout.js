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
	var Communicator	= require('communicator');
	var XAEnums 		= require('utils/XAEnums');
	var XALinks 		= require('modules/XALinks');
	var XAGlobals 		= require('utils/XAGlobals');
	var localization	= require('utils/XALangSupport');
	
	var XATablelayoutTmpl = require('hbs!tmpl/common/XATableLayout_tmpl');
	var Spinner		= require('views/common/Spinner');

	require('backgrid-filter');
	require('backgrid-paginator');

	var XATableLayout = Backbone.Marionette.Layout.extend(
	/** @lends XATableLayout */
	{
		_viewName : 'XATableLayout',
		
    	template: XATablelayoutTmpl,

		/** Layout sub regions */
    	regions: {
			'rTableList'	: 'div[data-id="r_tableList"]',
			'rTableSpinner'	: 'div[data-id="r_tableSpinner"]',
			'rPagination'	: 'div[data-id="r_pagination"]'
		},

    	// /** ui selector cache */
    	ui: {},

		gridOpts : {
			className: 'table table-bordered table-condensed backgrid',
			emptyText : 'No Records found!'
		},

		filterOpts : {
			placeholder: localization.tt('plcHldr.searchByResourcePath'),
			wait: 150
		},

		includePagination : true,

		includeFilter : false,

		includeHeaderSearch : false,

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			return events;
		},

    	/**
		* intialize a new HDXATableLayout Layout 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a XATableLayout Layout");
			/*
			 * mandatory :
			 * collection
			 * columns,
			 *
			 * optional :
			 * includePagination (true),
			 * includeFilter (false),
			 * includeHeaderSearch (false),
			 * gridOpts : {
			 * 	className: 'table table-bordered table-condensed backgrid',
			 *  row: TableRow,
			 *  header : XABackgrid,
			 *  emptyText : 'No Records found!',
			 * }
			 * filterOpts : {
			 *	name: ['name'],
			 *  placeholder: localization.tt('plcHldr.searchByResourcePath'),
			 *  wait: 150
			 * }
			 *
			 */
			_.extend(this, _.pick(options,	'collection', 'columns', 'includePagination', 
											'includeHeaderSearch', 'includeFilter' ,'scrollToTop', 'paginationCache'));

			_.extend(this.gridOpts, options.gridOpts, { collection : this.collection, columns : this.columns } );
			_.extend(this.filterOpts, options.filterOpts);
			
			this.scrollToTop = _.has(options, 'scrollToTop') ? options.scrollToTop : true;
			this.paginationCache = _.has(options, 'paginationCache') ? options.paginationCache : true;
			
			this.bindEvents();
		},

		/** all events binding here */
		bindEvents : function(){
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/

			this.listenTo(this.collection, "sync reset", this.showHidePager);
			
            this.listenTo(this.collection, 'request', function(){
				$(this.rTableSpinner.el).addClass('loading');
			},this);
            this.listenTo(this.collection, 'sync error', function(){
				$(this.rTableSpinner.el).removeClass('loading');
			},this);
		},

		/** on render callback */
		onRender: function() {
			this.initializePlugins();
			this.renderTable();
			if(this.includePagination) {
				this.renderPagination();
			}
			/*if(this.includeFilter){
				this.renderFilter();
			}*/
		},

		/** all post render plugin initialization */
		initializePlugins: function(){
		},

		renderTable : function(){
			var that = this;
			this.rTableList.show(new Backgrid.Grid(this.gridOpts));

			/*this.rTableSpinner.show(new Spinner({
				collection: this.collection
			}));*/

		},
		renderPagination : function(){
			this.rPagination.show(new Backgrid.Extension.Paginator({
				collection: this.collection,
				scrollToTop : this.scrollToTop,
				paginationCache : this.paginationCache
				
			}));
		},
		showHidePager : function(){
			if(this.collection.state && this.collection.state.totalRecords > XAGlobals.settings.PAGE_SIZE)	{
				$(this.rPagination.el).show();
			} else {
				$(this.rPagination.el).hide();
			}
		},
		renderFilter : function(){
			this.rFilter.show(new Backgrid.Extension.ServerSideFilter({
				  collection: this.collection,
				  name: ['name'],
				  placeholder: localization.tt('plcHldr.searchByResourcePath'),
				  wait: 150
			}));
			
			setTimeout(function(){
				that.$('table').colResizable({liveDrag :true});
			},0);
		},

		/** on close */
		onClose: function(){
		}

	});

	return XATableLayout; 
});
