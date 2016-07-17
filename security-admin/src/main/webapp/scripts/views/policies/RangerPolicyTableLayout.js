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

    var App				= require('App');
	var Backbone		= require('backbone');
	var XAEnums 		= require('utils/XAEnums');
	var XALinks 		= require('modules/XALinks');
	var XAGlobals 		= require('utils/XAGlobals');
	var SessionMgr 		= require('mgrs/SessionMgr');
	var XAUtil			= require('utils/XAUtils');
	
	var XABackgrid		= require('views/common/XABackgrid');
	var XATableLayout	= require('views/common/XATableLayout');
	var localization	= require('utils/XALangSupport');
	var RangerService		= require('models/RangerService');
	var RangerServiceDef	= require('models/RangerServiceDef');
	var RangerPolicy 		= require('models/RangerPolicy');
	var RangerPolicyTableLayoutTmpl = require('hbs!tmpl/policies/RangerPolicyTableLayout_tmpl');

	require('backgrid-filter');
	require('backgrid-paginator');
	require('bootbox');

	var RangerPolicyTableLayout = Backbone.Marionette.Layout.extend(
	/** @lends RangerPolicyTableLayout */
	{
		_viewName : 'RangerPolicyTableLayout',
		
    	template: RangerPolicyTableLayoutTmpl,

		templateHelpers : function(){
			return {
				rangerService:this.rangerService,
				rangerPolicyType : this.collection.queryParams['policyType']
			};
		},
        
    	breadCrumbs : function(){
    		if(this.rangerService.get('type') == XAEnums.ServiceType.SERVICE_TAG.label){
    			return [XALinks.get('TagBasedServiceManager'),XALinks.get('ManagePolicies',{model : this.rangerService})];
    		}
    		return [XALinks.get('ServiceManager'),XALinks.get('ManagePolicies',{model : this.rangerService})];
//    		return [];
   		},        

		/** Layout sub regions */
    	regions: {
			'rTableList'	: 'div[data-id="r_table"]',
		},

    	// /** ui selector cache */
    	ui: {
			'btnDeletePolicy' : '[data-name="deletePolicy"]',
			'btnShowMore' : '[data-id="showMore"]',
			'btnShowLess' : '[data-id="showLess"]',
			'visualSearch' : '.visual_search',
			'policyTypeTab' : 'div[data-id="policyTypeTab"]',
			'addNewPolicy' : '[data-js="addNewPolicy"]'
		},

		/** ui events hash */
		events: function() {
			var events = {};
			events['click ' + this.ui.btnDeletePolicy]  = 'onDelete';
			events['click ' + this.ui.btnShowMore]  = 'onShowMore';
			events['click ' + this.ui.btnShowLess]  = 'onShowLess';
			events['click ' + this.ui.policyTypeTab + ' ul li a']  = 'onTabChange';
			return events;
		},

    	/**
		* intialize a new RangerPolicyTableLayout Layout 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a RangerPolicyTableLayout Layout");

			_.extend(this, _.pick(options,'rangerService'));
			
			
			this.bindEvents();
			this.initializeServiceDef();
		},

		/** all events binding here */
		bindEvents : function(){
			//this.listenTo(this.collection, "sync", this.render, this);
		},
		initializeServiceDef : function(){
			this.rangerServiceDefModel	= new RangerServiceDef();
			this.rangerServiceDefModel.url = "service/plugins/definitions/name/"+this.rangerService.get('type');
			this.rangerServiceDefModel.fetch({
				cache : false,
				async : false
			})
		},
		
		initializePolicies : function(policyType){
			this.collection.url = XAUtil.getServicePoliciesURL(this.rangerService.id);
			if(!_.isUndefined(policyType)){
				this.collection.queryParams['policyType'] = policyType;
			}
			this.collection.fetch({
				cache : false,
			});
		},
		/** on render callback */
		onRender: function() {
			this.setTabForPolicyListing();
			this.addVisualSearch();
			this.renderTable();
			this.initializePolicies();
		},
		
		/** all post render plugin initialization */
		initializePlugins: function(){
		},
		setTabForPolicyListing : function(){
			var policyType = this.collection.queryParams['policyType']
			if( XAUtil.isMaskingPolicy(policyType) ){
				this.ui.policyTypeTab.find('ul li').removeClass('active');
				this.$el.find('li[data-tab="masking"]').addClass('active');
			}else if( XAUtil.isRowFilterPolicy(policyType) ){
				this.ui.policyTypeTab.find('ul li').removeClass('active');
				this.$el.find('li[data-tab="rowLevelFilter"]').addClass('active');
			}
			this.showRequiredTabs()
		},
		showRequiredTabs : function(){
			if(XAUtil.isEmptyObjectResourceVal(this.rangerServiceDefModel.get('dataMaskDef'))){
				this.$el.find('li[data-tab="masking"]').hide();
			}
			if(XAUtil.isEmptyObjectResourceVal(this.rangerServiceDefModel.get('rowFilterDef'))){
				this.$el.find('li[data-tab="rowLevelFilter"]').hide();
			}
		},
		renderTable : function(){
			var that = this;
			this.rTableList.show(new XATableLayout({
				columns: this.getColumns(),
				collection: this.collection,
				includeFilter : false,
				gridOpts : {
					row: Backgrid.Row.extend({}),
					header : XABackgrid,
					emptyText : 'No Policies found!'
				},
			}));
		},

		getColumns : function(){
			var that = this;
			var cols = {
				id : {
					cell : "uri",
					href: function(model){
						return '#!/service/'+that.rangerService.id+'/policies/'+model.id+'/edit';
					},
					label	: localization.tt("lbl.policyId"),
					editable: false,
					sortable : false
				},
				name : {
					cell : 'string',
					label	: localization.tt("lbl.policyName"),
					editable: false,
					sortable : false
				},	
				isEnabled:{
					label:localization.tt('lbl.status'),
					cell :"html",
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							return rawValue ? '<label class="label label-success">Enabled</label>' : '<label class="label label-important">Disabled</label>';
						}
					}),
					click : false,
					drag : false,
					sortable : false
				},
				isAuditEnabled:{
					label:localization.tt('lbl.auditLogging'),
					cell :"html",
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							return rawValue ? '<label class="label label-success">Enabled</label>' : '<label class="label label-important">Disabled</label>';
						}
					}),
					click : false,
					drag : false,
					sortable : false
				},
				policyItems : {
					reName : 'groupName',
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					label : localization.tt("lbl.group"),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							if(!_.isUndefined(rawValue)){
								return XAUtil.showGroupsOrUsersForPolicy(rawValue, model, true, that.rangerServiceDefModel);
							}
							return '--';
						}
					}),
					editable : false,
					sortable : false
				},
				//Hack for backgrid plugin doesn't allow to have same column name 
				users : {
					reName : 'userName',
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					label : localization.tt("lbl.users"),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
								return XAUtil.showGroupsOrUsersForPolicy(model.get('policyItems'), model, false, that.rangerServiceDefModel);
						}
					}),
					editable : false,
					sortable : false
				},
			};

			cols['permissions'] = {
				cell :  "html",
				label : localization.tt("lbl.action"),
				formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
					fromRaw: function (rawValue,model) {
						return '<a href="#!/service/'+that.rangerService.id+'/policies/'+model.id+'/edit" class="btn btn-mini" title="Edit"><i class="icon-edit icon-large" /></a>\
								<a href="javascript:void(0);" data-name ="deletePolicy" data-id="'+model.id+'"  class="btn btn-mini btn-danger" title="Delete"><i class="icon-trash icon-large" /></a>';
						//You can use rawValue to custom your html, you can change this value using the name parameter.
					}
				}),
				editable: false,
				sortable : false

			};
			return this.collection.constructor.getTableCols(cols, this.collection);
		},
		onDelete :function(e){
			var that = this;
			
			var obj = this.collection.get($(e.currentTarget).data('id'));
			var model = new RangerPolicy(obj.attributes);
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
							XAUtil.notifySuccess('Success', localization.tt('msg.policyDeleteMsg'));
							that.renderTable();
							that.collection.fetch();
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
			var attrName = 'policy-groups-id';
			var id = $(e.currentTarget).attr(attrName);
			if(_.isUndefined(id)){
				id = $(e.currentTarget).attr('policy-users-id');
				attrName = 'policy-users-id';
			}   
			var $td = $(e.currentTarget).parents('td');
			$td.find('['+attrName+'="'+id+'"]').show();
			$td.find('[data-id="showLess"]['+attrName+'="'+id+'"]').show();
			$td.find('[data-id="showMore"]['+attrName+'="'+id+'"]').hide();
			$td.find('[data-id="showMore"]['+attrName+'="'+id+'"]').parents('div[data-id="groupsDiv"]').addClass('set-height-groups');
		},
		onShowLess : function(e){
			var attrName = 'policy-groups-id';
			var id = $(e.currentTarget).attr(attrName);
			if(_.isUndefined(id)){
				id = $(e.currentTarget).attr('policy-users-id');
				attrName = 'policy-users-id';
			}
			var $td = $(e.currentTarget).parents('td');
			$td.find('['+attrName+'="'+id+'"]').slice(4).hide();
			$td.find('[data-id="showLess"]['+attrName+'="'+id+'"]').hide();
			$td.find('[data-id="showMore"]['+attrName+'="'+id+'"]').show();
			$td.find('[data-id="showMore"]['+attrName+'="'+id+'"]').parents('div[data-id="groupsDiv"]').removeClass('set-height-groups');
		},
		addVisualSearch : function(){
			var that = this;
			var resourceSearchOpt = _.map(this.rangerServiceDefModel.get('resources'), function(resource){ return XAUtil.capitaliseFirstLetter(resource.name) });
			var PolicyStatusValue = _.map(XAEnums.ActiveStatus, function(status) { return { 'label': status.label, 'value': Boolean(status.value)}; });
	
			var searchOpt = ['Policy Name','Group Name','User Name','Status'];//,'Start Date','End Date','Today'];
			searchOpt = _.union(searchOpt, resourceSearchOpt)
			var serverAttrName  = [{text : "Policy Name", label :"policyNamePartial"},{text : "Group Name", label :"group"},
			                       {text : "User Name", label :"user"}, {text : "Status", label :"isEnabled",'multiple' : true, 'optionsArr' : PolicyStatusValue}];
			                     // {text : 'Start Date',label :'startDate'},{text : 'End Date',label :'endDate'},
				                 //  {text : 'Today',label :'today'}];
			var serverRsrcAttrName = _.map(resourceSearchOpt,function(opt){ 
				return { 'text': XAUtil.capitaliseFirstLetter(opt), 
					'label': 'resource:'+XAUtil.lowerCaseFirstLetter(opt) }; 
			});
			serverAttrName = _.union(serverAttrName, serverRsrcAttrName)
			var pluginAttr = {
				      placeholder :localization.tt('h.searchForPolicy'),
				      container : this.ui.visualSearch,
				      query     : '',
				      callbacks :  { 
				    	  valueMatches :function(facet, searchTerm, callback) {
								switch (facet) {
									case 'Status':
										callback(that.getActiveStatusNVList());
										break;
									case 'Policy Type':
										callback(that.getNameOfPolicyTypeNVList());
//										callback(XAUtil.enumToSelectLabelValuePairs(XAEnums.PolicyType));
										break;		
								/*	case 'Audit Status':
										callback(XAUtil.enumToSelectLabelValuePairs(XAEnums.AuthType));
										break;	
									case 'Start Date' :
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
			window.vs = XAUtil.addVisualSearch(searchOpt,serverAttrName, this.collection,pluginAttr);
		},
		getActiveStatusNVList : function() {
			var activeStatusList = _.filter(XAEnums.ActiveStatus, function(obj){
				if(obj.label != XAEnums.ActiveStatus.STATUS_DELETED.label)
					return obj;
			});
			return _.map(activeStatusList, function(status) { return { 'label': status.label, 'value': status.label}; })
		},
		getNameOfPolicyTypeNVList : function() {
			return _.map(XAEnums.PolicyType, function(type) { return { 'label': type.label, 'value': type.label};});
		},
		onTabChange : function(e){
			var that = this, 
			tab = $(e.currentTarget).attr('href');
			var href = this.ui.addNewPolicy.attr('href')
			switch (tab) {
				case "#access":
					var val = XAEnums.RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value;
					App.appRouter.navigate("#!/service/"+this.rangerService.id+"/policies/"+ val,{trigger: true});
					break;
				case "#masking":
					var val = XAEnums.RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value;
					App.appRouter.navigate("#!/service/"+this.rangerService.id+"/policies/"+ val,{trigger: true});
					break;
				case "#rowLevelFilter":
					var val = XAEnums.RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value;
					App.appRouter.navigate("#!/service/"+this.rangerService.id+"/policies/"+ val,{trigger: true});
					break;
			}
		},
		/** on close */
		onClose: function(){
		}

	});

	return RangerPolicyTableLayout; 
});
