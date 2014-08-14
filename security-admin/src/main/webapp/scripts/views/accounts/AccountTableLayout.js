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

define(function(require) {'use strict';

	var Backbone = require('backbone');
	var Communicator = require('communicator');
	
	var XALinks = require('modules/XALinks');
	var XAEnums 	= require('utils/XAEnums');
	var localization = require('utils/XALangSupport');
	var XABackgrid		= require('views/common/XABackgrid');
	var XATableLayout	= require('views/common/XATableLayout');

	var AccounttablelayoutTmpl = require('hbs!tmpl/accounts/AccountTableLayout_tmpl');

	require('backgrid');
	require('backgrid-paginator');
	//require('backbone-pageable');
	require('jquery-toggles');

	var AccountTableLayout = Backbone.Marionette.Layout.extend(
	/** @lends AccountTableLayout */
	{
		_viewName : 'AccountTableLayout',

		template : AccounttablelayoutTmpl,
		breadCrumbs : [XALinks.get('Accounts')],

		/** Layout sub regions */
		regions : {
			'rTableList' : 'div[data-id="r_tableList"]',
			'rPagination' : 'div[data-id="r_pagination"]'
		},

		/** ui selector cache */
		ui : {},

		/** ui events hash */
		events : function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			return events;
		},

		/**
		 * intialize a new AccountTableLayout Layout
		 * @constructs
		 */
		initialize : function(options) {
			console.log("initialized a AccountTableLayout Layout");

			_.extend(this, _.pick(options, ''));

			this.bindEvents();
		},

		/** all events binding here */
		bindEvents : function() {
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
		},

		/** on render callback */
		onRender : function() {
			this.initializePlugins();
			this.renderTable();
		},

		/** all post render plugin initialization */
		initializePlugins : function() {
		},

		renderTable : function() {
			/*if(! this.collection.length){
			 return;
			 }*/
			/*var TableRow = Backgrid.Row.extend({
			});

			this.rTableList.show(new Backgrid.Grid({
				className: 'table table-bordered table-condensed backgrid',
				columns : this.getColumns(),
				collection : this.collection,
				row : TableRow
			}));
			

			this.rPagination.show(new Backgrid.Extension.Paginator({
				collection : this.collection,
				className: "pagination",
				controls : {
					rewind : {
						label : "«",
						title : "First"
					},
					back : {
						label : "‹",
						title : "Previous"
					},
					forward : {
						label : "›",
						title : "Next"
					},
					fastForward : {
						label : "»",
						title : "Last"
					}
				},
			}));*/
			this.rTableList.show(new XATableLayout({
				columns: this.getColumns(),
				collection: this.collection,
				includeFilter : false,
				gridOpts : {
					header : XABackgrid,
					emptyText : 'No Accounts found!'
				}
			}));
		},

		getColumns : function() {
			var cols = {
				//id : {},
				customerName : {
					label : "Customer Name",
					cell : "uri",
					href: function(model){
						return '#!/account/'+model.get('id')+'/user/' + 1;
					}
				},
				accountStatus : {
					label : "Status",
					/*cell :"Switch",
					formatter : _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw : function(rawValue) {
							var status;
							_.each(_.toArray(XAEnums.BooleanValue),function(m){
								if(parseInt(rawValue) == m.value){
									status =  (m.label == XAEnums.BooleanValue.BOOL_TRUE.label) ? true : false;
									return ;
								}	
							});
							//You can use rawValue to custom your html, you can change this value using the name parameter.
							return status;
						}
					}),*/
					formatter : _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw : function(rawValue) {
							return rawValue == XAEnums.ActiveStatus.STATUS_ENABLED.value ? XAEnums.ActiveStatus.STATUS_ENABLED.label: XAEnums.ActiveStatus.STATUS_DISABLED.label;
						}
					})
				}
				/*action : {
					cell :"uri",
					label : localization.tt("lbl.action"),
					href: function(model){
						return '#!/accounts/' + model.id+'/edit';
					},
					title : 'Edit',
					editable:false,
					iconKlass :'icon-edit',
					iconTitle :'Edit'
					

				}*/
			};
			return this.collection.constructor.getTableCols(cols, this.collection);
		},

		/** on close */
		onClose : function() {
		}
	});

	return AccountTableLayout;
});
