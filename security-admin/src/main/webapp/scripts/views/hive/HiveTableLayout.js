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
	var XAGlobals 		= require('utils/XAGlobals');
	var SessionMgr 		= require('mgrs/SessionMgr');
	var XAUtil			= require('utils/XAUtils');

	var VXResource = require('models/VXResource');
	
	var XABackgrid		= require('views/common/XABackgrid');
	var XATableLayout	= require('views/common/XATableLayout');
	var localization	= require('utils/XALangSupport');
	var vFolderInfo 		= require('views/folders/FolderInfo');
	var HivetablelayoutTmpl = require('hbs!tmpl/hive/HiveTableLayout_tmpl');

	var HiveTableLayout = Backbone.Marionette.Layout.extend(
	/** @lends HiveTableLayout */
	{
		_viewName : 'HiveTableLayout',
		
    	template: HivetablelayoutTmpl,
    	templateHelpers :function(){
    		return {
    			isSysAdmin :this.isSysAdmin,
    			assetId		  : this.assetModel.id,
    			assetModel : this.assetModel,
    			version : XAGlobals.version
    		};
    	},
    	breadCrumbs : function(){
    		return [XALinks.get('RepositoryManager'),XALinks.get('ManageHivePolicies',{model : this.assetModel})];
   		},
		/** Layout sub regions */
    	regions: {
    		'rTableList' :'div[data-id="r_tableList"]',
    		'rPolicyDetail'	: '#policyDetail'
    	},

    	/** ui selector cache */
    	ui: {
			'btnExport' : 'a[data-js="export"]',
			'btnDeletePolicy' : '[data-name="deletePolicy"]',
			'btnShowMore' : '[data-id="showMore"]',
			'btnShowLess' : '[data-id="showLess"]',
			'visualSearch' : '.visual_search'
		},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			events['click ' + this.ui.btnExport]  = 'onExport';
			events['click ' + this.ui.btnDeletePolicy]  = 'onDelete';
			events['click ' + this.ui.btnShowMore]  = 'onShowMore';
			events['click ' + this.ui.btnShowLess]  = 'onShowLess';
			return events;
		},

    	/**
		* intialize a new HiveTableLayout Layout 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a HiveTableLayout Layout");

			_.extend(this, _.pick(options, 'assetModel'));
			this.collection.extraSearchParams = {
	//				resourceType : XAEnums.AssetType.ASSET_HIVE.value
					assetId : this.assetModel.id
				};
			this.bindEvents();
			this.isSysAdmin = SessionMgr.isSystemAdmin();
		},

		/** all events binding here */
		bindEvents : function(){
			//this.listenTo(this.collection, "remove", this.render, this);
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
		},

		/** on render callback */
		onRender: function() {
			var that = this;
			this.initializePlugins();
			this.addVisualSearch();
			this.renderTable();
			XAUtil.highlightDisabledPolicy(this);
		},

		/** all post render plugin initialization */
		initializePlugins: function(){
		},
		renderTable : function(){
			var that = this;
			/*var tableRow = Backgrid.Row.extend({
				events: {
					'click' : 'onClick'
				},
				initialize : function(){
					var that = this;
					var args = Array.prototype.slice.apply(arguments);
					Backgrid.Row.prototype.initialize.apply(this, args);
					this.listenTo(this.model, 'model1:highlightBackgridRow', function(){
						that.$el.addClass("alert");
						$("html, body").animate({scrollTop: that.$el.position().top},"linear");
						setTimeout(function () {
							that.$el.removeClass("alert");
						}, 1500);
					}, this);
				},
				onClick: function (e) {
					if($(e.toElement).is('.icon-edit,.icon-trash,a,code'))
						return;
					this.$el.parent('tbody').find('tr').removeClass('tr-active');
					this.$el.toggleClass('tr-active');
					if(that.rPolicyDetail){
						$(that.rPolicyDetail.el).hide();
						$(that.rPolicyDetail.el).html(new vFolderInfo({
							model : this.model
						}).render().$el).slideDown();
					}	
									
				}
			});*/
			this.rTableList.show(new XATableLayout({
				columns: this.getColumns(),
				collection: this.collection,
				includeFilter : false,
				gridOpts : {
//					row: tableRow,
					header : XABackgrid,
					emptyText : 'No Policies found!'
				}
			}));	
		},

		getColumns : function(){
			var cols = {
				policyName : {
					cell : "uri",
					href: function(model){
						return '#!/hive/'+model.get('assetId')+'/policy/' + model.id;
					},
					label	: localization.tt("lbl.policyName"),
					editable: false,
					sortable : false
				},
				databases : {
					cell :'html',
					label	: localization.tt("lbl.databaseName")+'(s)',
					editable:false,
					sortable : false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							return _.isUndefined(rawValue) ? '--': '<span title="'+rawValue+'">'+rawValue+'</span>';  
						}
					})
				},
				tables : {
					label	: localization.tt("lbl.tableName")+'(s)',
					cell :'html',
					editable:false,
					sortable : false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							return _.isUndefined(rawValue) ? '--': '<span title="'+rawValue+'">'+rawValue+'</span>';  
						}
					})
				},
				tableType:{
					label:localization.tt('lbl.tableType'),
					cell :"html",
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							var isInclude;
							if(_.isUndefined(rawValue) || !_.isEmpty(model.get('udfs')))
								return '--';
							_.each(_.toArray(XAEnums.PolicyType),function(m){
								if(parseInt(rawValue) == m.value){
									isInclude =  (m.label == XAEnums.PolicyType.POLICY_INCLUSION.label) ? true : false;
								}	
							});
							return isInclude ? '<label class="label label-success">Include</label>' : '<label class="label label">Exclude</label>';
						}
					}),
					click : false,
					drag : false,
					sortable : false
				},
				udfs : {
					label	: localization.tt("lbl.udfName")+'(s)',
					editable:false,
					cell :'string',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {	return rawValue ? rawValue : '--';}
					}),
					sortable : false
				},
				columns : {
					label	: localization.tt("lbl.columnName")+'(s)',
					cell :'html',
					placeholder : 'Column Name',
					editable:false,
					sortable : false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							return _.isUndefined(rawValue) ? '--': '<span title="'+rawValue+'">'+rawValue+'</span>';  
						}
					})

				},
				columnType:{
					label:localization.tt('lbl.columnType'),
					cell :"html",
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							if(_.isUndefined(rawValue) || !_.isEmpty(model.get('udfs')))
								return '--';
							var isInclude;
							_.each(_.toArray(XAEnums.PolicyType),function(m){
								if(parseInt(rawValue) == m.value){
									isInclude =  (m.label == XAEnums.PolicyType.POLICY_INCLUSION.label) ? true : false;
								}	
							});
							return isInclude ? '<label class="label label-success">Include</label>' : '<label class="label labe">Exclude</label>';
						}
					}),
					click : false,
					drag : false,
					sortable : false

				},
				permMapList : {
					reName : 'groupName',
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					label : localization.tt("lbl.group"),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							if(!_.isUndefined(rawValue))
								return XAUtil.showGroups(rawValue);
							else 
								return '--';
						}
					}),
					editable : false,
					sortable : false
				},
				auditList : {
					label : localization.tt("lbl.auditLogging"),
					cell: "html",
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							return model.has('auditList') ? '<label class="label label-success">ON</label>' : '<label class="label label">OFF</label>';
						}
					}),
					click : false,
					drag : false,
					sortable : false,
					editable : false
				},
				/*isEncrypt:{
					label:localization.tt("lbl.encrypted"),
					cell :"Switch",
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
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
					}),
					click : false,
					drag : false,
					onText : 'ON',
					offText : 'OFF',
					sortable : false
				},*/
				resourceStatus:{
					label:localization.tt('lbl.status'),
					cell :"html",
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							var status;
							_.each(_.toArray(XAEnums.BooleanValue),function(m){
								if(parseInt(rawValue) == m.value){
									status =  (m.label == XAEnums.BooleanValue.BOOL_TRUE.label) ? true : false;
								}	
							});
							return status ? '<label class="label label-success">Enabled</label>' : '<label class="label label-important">Disabled</label>';
						}
					}),
					click : false,
					drag : false,
					sortable : false
				},
/*				policyType:{
					label:localization.tt('lbl.policyType'),
					cell :"html",
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							var isInclude;
							_.each(_.toArray(XAEnums.PolicyType),function(m){
								if(parseInt(rawValue) == m.value){
									isInclude =  (m.label == XAEnums.PolicyType.POLICY_INCLUSION.label) ? true : false;
								}	
							});
							return isInclude ? '<label class="label label-success">Include</label>' : '<label class="label label-important">Exclude</label>';
						}
					}),
					click : false,
					drag : false,
					sortable : false
				},*/
				permissions : {
					cell :  "html",
					label : localization.tt("lbl.action"),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue,model) {
							return '<a href="#!/hive/'+model.get('assetId')+'/policy/'+model.id+'" class="btn btn-mini" title="Edit"><i class="icon-edit icon-large" /></a>\
									<a href="javascript:void(0);" data-name ="deletePolicy" data-id="'+model.id+'"  class="btn btn-mini btn-danger" title="Delete"><i class="icon-trash icon-large" /></a>';
							//You can use rawValue to custom your html, you can change this value using the name parameter.
						}
					}),
					editable:false,
					sortable : false	
				}
				
				
			};
			return this.collection.constructor.getTableCols(cols, this.collection);
		},

		onExport : function(e){
			$(e.currentTarget).attr('href', 'service/assets/policyList/'+this.assetModel.get('name')+'?epoch=0');
			//$(e.currentTarget).attr('href', 'service/assets/hive?epoch=0');
		},
		onDelete :function(e){
			var that = this;
			var obj = this.collection.get($(e.currentTarget).data('id'));
			var model = new VXResource(obj.attributes);
			model.collection = this.collection;
			XAUtil.confirmPopup({
				//msg :localize.tt('msg.confirmDelete'),
				msg :'Are you sure want to delete ?',
				callback : function(){
					XAUtil.blockUI();
					model.destroy({
						success: function(model, response) {
							XAUtil.blockUI('unblock');
							that.collection.remove(model.get('id'));
							$(that.rPolicyDetail.el).hide();
							XAUtil.notifySuccess('Success', localization.tt('msg.policyDeleteMsg'));
							if(that.collection.length ==  0){
								that.renderTable();
								that.collection.fetch();
							}
						},
						error: function (model, response, options) {
							XAUtil.blockUI('unblock');
							if ( response && response.responseJSON && response.responseJSON.msgDesc){
								    XAUtil.notifyError('Error', response.responseJSON.msgDesc);
							    }else
							    	XAUtil.notifyError('Error', 'Error deleting Policy!');
							    console.log("error");
						}
					});
				}
			});
		},
		onShowMore : function(e){
			var id = $(e.currentTarget).attr('policy-group-id');
			this.rTableList.$el.find('[policy-group-id="'+id+'"]').show();
			$('[data-id="showLess"][policy-group-id="'+id+'"]').show();
			$('[data-id="showMore"][policy-group-id="'+id+'"]').hide();
		},
		onShowLess : function(e){
			var id = $(e.currentTarget).attr('policy-group-id');
			this.rTableList.$el.find('[policy-group-id="'+id+'"]').slice(4).hide();
			$('[data-id="showLess"][policy-group-id="'+id+'"]').hide();
			$('[data-id="showMore"][policy-group-id="'+id+'"]').show();
		},
		addVisualSearch : function(){
			var that = this;
			var searchOpt = ['Database Name(s)','Table Name(s)','Column Name(s)','UDF Name(s)','Group','Policy Name'];//,'Start Date','End Date','Today'];
			var serverAttrName  = [{text : "Database Name(s)", label :"databases"}, {text : "Table Name(s)", label :"tables"},
			                       {text : "Column Name(s)", label :"columns"}, {text : "UDF Name(s)", label :"udfs"},
			                       {text : "Group", label :"groupName"},{text : "Policy Name", label :"policyName"}];
			                     //  {text : 'Start Date',label :'startDate'},{text : 'End Date',label :'endDate'},
				                 //  {text : 'Today',label :'today'}];
									
			var pluginAttr = {
				      placeholder :localization.tt('h.searchForPolicy'),
				      container : this.ui.visualSearch,
				      query     : '',
				      callbacks :  { 
				    	  valueMatches :function(facet, searchTerm, callback) {
								switch (facet) {
									case 'Result':
										callback(XAUtil.enumToSelectLabelValuePairs(XAEnums.AuthStatus));
										break;
									case 'Login Type':
										callback(XAUtil.enumToSelectLabelValuePairs(XAEnums.AuthType));
										break;	
								/*	case 'Start Date' :
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
			XAUtil.addVisualSearch(searchOpt,serverAttrName, this.collection,pluginAttr);
		},

		/** on close */
		onClose: function(){
		}

	});

	return HiveTableLayout; 
});
