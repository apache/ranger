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

 
define(function(require) {
	'use strict';

	var Backbone 		= require('backbone');
	var App 			= require('App');

	var XAEnums 		= require('utils/XAEnums');
	var XAGlobals 		= require('utils/XAGlobals');
	var XAUtils			= require('utils/XAUtils');
	var XABackgrid		= require('views/common/XABackgrid');
	var XATableLayout	= require('views/common/XATableLayout');
	var localization	= require('utils/XALangSupport');
	var SessionMgr 		= require('mgrs/SessionMgr');
	
	var VXAuthSession				= require('collections/VXAuthSessionList');
	var VXTrxLogList   				= require('collections/VXTrxLogList');
	var VXAssetList 				= require('collections/VXAssetList');
	var VXPolicyExportAuditList 	= require('collections/VXPolicyExportAuditList');
	var RangerServiceDefList 		= require('collections/RangerServiceDefList');
	var RangerService				= require('models/RangerService');
	var RangerServiceList			= require('collections/RangerServiceList');
	var AuditlayoutTmpl 			= require('hbs!tmpl/reports/AuditLayout_tmpl');
	var vOperationDiffDetail		= require('views/reports/OperationDiffDetail');
	var RangerPolicy 				= require('models/RangerPolicy');
	var RangerPolicyRO				= require('views/policies/RangerPolicyRO');
	var vPlugableServiceDiffDetail	= require('views/reports/PlugableServiceDiffDetail');

	require('moment');
	require('bootstrap-datepicker');
	require('Backbone.BootstrapModal');
	require('visualsearch');
	
	var AuditLayout = Backbone.Marionette.Layout.extend(
	/** @lends AuditLayout */
	{
		_viewName : 'AuditLayout',

		template : AuditlayoutTmpl,
		templateHelpers : function(){
			var repositoryList = _.map(XAEnums.AssetType,function(asset){return {id : asset.value, value :asset.label};});
			
			return {
				repositoryList	: repositoryList,
				currentDate 	: Globalize.format(new Date(),  "MM/dd/yyyy hh:mm:ss tt")	
			};
		},
		breadCrumbs : [],

		/** Layout sub regions */
		regions : {
			'rTableList'	: 'div[data-id="r_tableLists"]'
		},

		/** ui selector cache */
		ui : {
			tableList			: 'div[data-id="r_tableLists"]',
			refresh 			: '[data-id="refresh"]',
			resourceName 		: '[data-id = "resourceName"]',
			selectRepo	 		: '[data-id = "selectRepo"]',
			searchBtn 			: '[data-id="searchBtn"]',
			lastUpdateTimeLabel : '[data-id = "lastUpdateTimeLabel"]',
			startDate	 		: '[data-id = "startDate"]',
			endDate	 			: '[data-id = "endDate"]',
			tab 				: '.nav-tabs',
			refreshTable		: '[data-id="refreshTable"]',
			quickFilter			: '[data-id="quickFilter"]',
			visualSearch		: '.visual_search'
			
		},

		/** ui events hash */
		events : function() {
			var events = {};
			events['click ' + this.ui.refresh]         = 'onRefresh';
			events['click ' + this.ui.searchBtn]  	   = 'onSearch';
			events['click '+this.ui.tab+' a']		   = 'onTabChange';
			return events;
		},

		/**
		 * intialize a new AuditLayout Layout
		 * @constructs
		 */
		initialize : function(options) {
			console.log("initialized a AuditLayout Layout");

			_.extend(this, _.pick(options, 'accessAuditList','tab'));
			this.bindEvents();
			this.currentTab = '#'+this.tab;
			var date = new Date().toString();
			this.timezone = date.replace(/^.*GMT.*\(/, "").replace(/\)$/, "");
			this.initializeServiceDefColl();
		},

		/** all events binding here */
		bindEvents : function() {
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			//this.listenTo(this.collection, "change:foo", this.render, this);
		},
		initializeCollection : function(){
			this.collection.fetch({
				reset : true,
				cache : false
			});
		},
		initializeServiceDefColl : function() {
			this.serviceDefList	= new RangerServiceDefList();
			this.serviceDefList.fetch({ 
				cache : false,
				async:false
			});
			return this.serviceDefList;
		},
		/** on render callback */
		onRender : function() {
			this.initializePlugins();
			if(this.currentTab != '#bigData'){
				this.onTabChange();
				this.ui.tab.find('li[class="active"]').removeClass();
				this.ui.tab.find('[href="'+this.currentTab+'"]').parent().addClass('active');
			} else {
				this.renderBigDataTable();
				this.addSearchForBigDataTab();
				this.modifyTableForSubcolumns();
			}
		 },
		displayDatepicker : function ($el, callback) {
			var input = $el.find('.search_facet.is_editing input.search_facet_input');
		    
		    input.datepicker({
		    	autoclose : true,
		    	dateFormat: 'yy-mm-dd'
		    }).on('changeDate', function(ev){
		    	callback(ev.date);
		    	input.datepicker("hide");
		    	var e = jQuery.Event("keydown");
		    	e.which = 13; // Enter
		    	$(this).trigger(e);
		    }).on('hide',function(){
		    	input.datepicker("destroy");
		    });
		    input.datepicker('show');
		},
		modifyTableForSubcolumns : function(){
			this.$el.find('[data-id="r_tableList"] table thead').prepend('<tr>\
					<th class="renderable pid"></th>\
					<th class="renderable ruser"></th>\
					<th class="renderable ruser"></th>\
					<th class="renderable cip">Service</th>\
					<th class="renderable name">Resource</th>\
					<th class="renderable cip"></th>\
					<th class="renderable cip"></th>\
					<th class="renderable cip"> </th>\
					<th class="renderable aip" > </th>\
					<th class="renderable aip" > </th>\
					<th class="renderable ruser"></th>\
				</tr>');
		},
		renderDateFields : function(){
			var that = this;

			this.ui.startDate.datepicker({
				autoclose : true
			}).on('changeDate', function(ev) {
				that.ui.endDate.datepicker('setStartDate', ev.date);
			}).on('keydown',function(){
				return false;
			});
			
			this.ui.endDate.datepicker({
				autoclose : true
			}).on('changeDate', function(ev) {
				that.ui.startDate.datepicker('setEndDate', ev.date);
			}).on('keydown',function(){
				return false;
			});
		},
		onTabChange : function(e){
			var that = this, tab;
			tab = !_.isUndefined(e) ? $(e.currentTarget).attr('href') : this.currentTab;
			this.$el.parents('body').find('.datepicker').remove();
			switch (tab) {
				case "#bigData":
					this.currentTab = '#bigData';
					this.ui.visualSearch.show();
					this.ui.visualSearch.parents('.well').show();
					this.renderBigDataTable();
					this.modifyTableForSubcolumns();
					this.addSearchForBigDataTab();
					this.listenTo(this.accessAuditList, "request", that.updateLastRefresh)
					break;
				case "#admin":
					this.currentTab = '#admin';
					this.trxLogList = new VXTrxLogList();
					this.renderAdminTable();
					if(_.isUndefined(App.sessionId)){
			     	    this.trxLogList.fetch({
							   cache : false
						});
					}
					this.addSearchForAdminTab();
					this.listenTo(this.trxLogList, "request", that.updateLastRefresh)
					break;
				case "#loginSession":
					this.currentTab = '#loginSession';
					this.authSessionList = new VXAuthSession();
					this.renderLoginSessionTable();
					//Setting SortBy as id and sortType as desc = 1
					this.authSessionList.setSorting('id',1); 
					this.authSessionList.fetch({
						cache:false,
					});
					this.addSearchForLoginSessionTab();
					this.listenTo(this.authSessionList, "request", that.updateLastRefresh)
					break;
				case "#agent":
					this.currentTab = '#agent';
					this.policyExportAuditList = new VXPolicyExportAuditList();	
					var params = { priAcctId : 1 };
					that.renderAgentTable();
					this.policyExportAuditList.setSorting('createDate',1);
					this.policyExportAuditList.fetch({
						cache : false,
						data :params
					});
					this.addSearchForAgentTab();
					this.listenTo(this.policyExportAuditList, "request", that.updateLastRefresh)
					break;	
			}
			var lastUpdateTime = Globalize.format(new Date(),  "MM/dd/yyyy hh:mm:ss tt");
			that.ui.lastUpdateTimeLabel.html(lastUpdateTime);
		},
		addSearchForBigDataTab :function(){
			var that = this;
			var serverListForRepoType =  this.serviceDefList.map(function(serviceDef){ return {'label' : serviceDef.get('name').toUpperCase(), 'value' : serviceDef.get('id')}; })
			var serverAttrName = [{text : 'Start Date',label :'startDate'},{text : 'End Date',label :'endDate'},
			                      {text : 'Today',label :'today'},{text : 'User',label :'requestUser'},
			                      {text : 'Resource Name',label :'resourcePath'},{text : 'Policy ID',label :'policyId'},
			                      {text : 'Service Name',label :'repoName'},
			                      {text : 'Service Type',label :'repoType','multiple' : true, 'optionsArr' : serverListForRepoType},
			                      {text : 'Result',label :'accessResult', 'multiple' : true, 'optionsArr' : XAUtils.enumToSelectLabelValuePairs(XAEnums.AccessResult)},
			                      {text : 'Access Type',label :'accessType'},{text : 'Access Enforcer',label :'aclEnforcer'},
			                      {text : 'Audit Type',label :'auditType'},{text : 'Session ID',label :'sessionId'},
			                      {text : 'Client IP',label :'clientIP'},{text : 'Client Type',label :'clientType'},
			                      {text : 'Tags',label :'tags'},
			                      {text : 'Resource Type',label : 'resourceType'}];
            var searchOpt = ['Resource Type','Start Date','End Date','User','Service Name','Service Type','Resource Name','Access Type','Result','Access Enforcer','Client IP','Tags'];//,'Policy ID'
            this.clearVisualSearch(this.accessAuditList, serverAttrName);
            
			//'Resource Type','Audit Type','Session IP','Client Type','Today',
            var query = '"Start Date": "'+Globalize.format(new Date(),"MM/dd/yyyy")+'"';
			var pluginAttr = {
			      placeholder :localization.tt('h.searchForYourAccessAudit'),
			      container : this.ui.visualSearch,
			      query     : query,
			      callbacks :  { 
					valueMatches : function(facet, searchTerm, callback) {
						var auditList = [];
						_.each(XAEnums.ClassTypes, function(obj){
							if((obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_ASSET.value) 
									|| (obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_RESOURCE.value) 
									|| (obj.value == XAEnums.ClassTypes.CLASS_TYPE_RANGER_POLICY.value) 
									|| (obj.value == XAEnums.ClassTypes.CLASS_TYPE_RANGER_SERVICE.value)
									|| (obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_USER.value) 
									|| (obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_GROUP.value))
								auditList.push({label :obj.label, value :obj.value+''});
						});
						
						switch (facet) {
							case 'Service Name':
								var serviceList 	= new RangerServiceList();
								serviceList.setPageSize(100);
								serviceList.fetch().done(function(){
									callback(serviceList.map(function(model){return model.get('name');}));
								});
								break;
							case 'Service Type':
								var serviceList =  that.serviceDefList.map(function(serviceDef){ return {'label' : serviceDef.get('name').toUpperCase(), 'value' : serviceDef.get('name').toUpperCase()}; })
								callback(serviceList);
								break;
							case 'Result':
				                callback(XAUtils.hackForVSLabelValuePairs(XAEnums.AccessResult));
				                break;  
							case 'Start Date' :
								var endDate, models = that.visualSearch.searchQuery.where({category:"End Date"});
								if(models.length > 0){
									var tmpmodel = models[0];
									endDate = tmpmodel.attributes.value;
								} 
								XAUtils.displayDatepicker(that.ui.visualSearch, facet, endDate, callback);
								break;
							case 'End Date' :
								var startDate, models = that.visualSearch.searchQuery.where({category:"Start Date"});
								if(models.length > 0){
									var tmpmodel = models[0];
									startDate = tmpmodel.attributes.value;
								} 
								XAUtils.displayDatepicker(that.ui.visualSearch, facet, startDate, callback);
								break;
							case 'Today'	:
								var today = Globalize.format(new Date(),"yyyy/mm/dd");
								callback([today]);
								break;
				            }
					}
			      }
			};
			this.visualSearch = XAUtils.addVisualSearch(searchOpt,serverAttrName, this.accessAuditList, pluginAttr);
		},
		addSearchForAdminTab : function(){
			var that = this;
			var searchOpt = ["Operation", "Audit Type", "User", "Date", "Actions", "Session Id"];
			searchOpt = _.without(searchOpt,'Date','Operation');
			searchOpt = _.union(searchOpt, ['Start Date','End Date']);//'Today'
			var serverAttrName  = [{text : "Operation", label :"objectClassName"}, 
			                       {text : "Audit Type", label :"objectClassType",'multiple' : true, 'optionsArr' : XAUtils.enumToSelectLabelValuePairs(XAEnums.ClassTypes)},
			                       {text : "User", label :"owner"},{text : "Date", label :"startDate"},
			                       {text : "Actions", label :"action"},{text :  "Session Id", label :"sessionId"},
			                       {text : 'Start Date',label :'startDate'},{text : 'End Date',label :'endDate'},
   			                       {text : 'Today',label :'today'}
			                       ];
			
			var auditList = [],query = '';
			_.each(XAEnums.ClassTypes, function(obj){
				if((obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_ASSET.value) 
						|| (obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_RESOURCE.value) 
						|| (obj.value == XAEnums.ClassTypes.CLASS_TYPE_RANGER_POLICY.value) 
						|| (obj.value == XAEnums.ClassTypes.CLASS_TYPE_RANGER_SERVICE.value) 
						|| (obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_USER.value)  
						|| (obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_GROUP.value))
					auditList.push({label :obj.label, value :obj.label+''});
			});
			if(!_.isUndefined(App.sessionId)){
				query = '"Session Id": "'+App.sessionId+'"';
				delete App.sessionId;
			}
			var pluginAttr = {
				      placeholder :localization.tt('h.searchForYourAccessLog'),
				      container : this.ui.visualSearch,
				      query     : query,
				      callbacks :  { 
				    	  valueMatches :function(facet, searchTerm, callback) {
								switch (facet) {
									case 'Audit Type':
										callback(auditList);
										break;
									case 'Actions':
										callback(["create","update","delete","password change"]);
										break;
									case 'Start Date' :
											var endDate, models = that.visualSearch.searchQuery.where({category:"End Date"});
											if(models.length > 0){
												var tmpmodel = models[0];
												endDate = tmpmodel.attributes.value;
											} 
											XAUtils.displayDatepicker(that.ui.visualSearch, facet, endDate, callback);
											break;
									case 'End Date' :
										var startDate, models = that.visualSearch.searchQuery.where({category:"Start Date"});
										if(models.length > 0){
											var tmpmodel = models[0];
											startDate = tmpmodel.attributes.value;
										} 
										XAUtils.displayDatepicker(that.ui.visualSearch, facet, startDate, callback);
										break;
									case 'Today'	:
										var today = Globalize.format(new Date(),"yyyy/mm/dd");
										callback([today]);
										break;
										
								}     
			            	
							}
				      }
				};
			
			this.visualSearch = XAUtils.addVisualSearch(searchOpt,serverAttrName, this.trxLogList,pluginAttr);
		},
		addSearchForLoginSessionTab : function(){
			var that = this;
			var searchOpt = ["Session Id", "Login Id", "Result", "Login Type", "IP", "User Agent", "Login Time"];
			searchOpt = _.without(searchOpt,'Login Time');
			searchOpt = _.union(searchOpt, ['Start Date','End Date']);//'Today'
			var serverAttrName  = [{text : "Session Id", label :"id"}, {text : "Login Id", label :"loginId"},
			                       {text : "Result", label :"authStatus",'multiple' : true, 'optionsArr' : XAUtils.enumToSelectLabelValuePairs(XAEnums.AuthStatus)},
			                       {text : "Login Type", label :"authType",'multiple' : true, 'optionsArr' : XAUtils.enumToSelectLabelValuePairs(XAEnums.AuthType)},
			                       {text : "IP", label :"requestIP"},{text :"User Agent", label :"requestUserAgent"},{text : "Login Time", label :"authTime"},
			                       {text : 'Start Date',label :'startDate'},{text : 'End Date',label :'endDate'},
				                   {text : 'Today',label :'today'}];
									
			var pluginAttr = {
				      placeholder :localization.tt('h.searchForYourLoginSession'),
				      container : this.ui.visualSearch,
				      query     : '',
				      callbacks :  { 
				    	  valueMatches :function(facet, searchTerm, callback) {
								switch (facet) {
									case 'Result':
										var authStatusList = _.filter(XAEnums.AuthStatus, function(obj){
											if(obj.label != XAEnums.AuthStatus.AUTH_STATUS_UNKNOWN.label)
												return obj;
										});
										callback(XAUtils.hackForVSLabelValuePairs(authStatusList));
										break;
									case 'Login Type':
										var authTypeList = _.filter(XAEnums.AuthType, function(obj){
											if(obj.label != XAEnums.AuthType.AUTH_TYPE_UNKNOWN.label)
												return obj;
										});
										callback(XAUtils.hackForVSLabelValuePairs(authTypeList));
										break;	
									case 'Start Date' :
											var endDate, models = that.visualSearch.searchQuery.where({category:"End Date"});
											if(models.length > 0){
												var tmpmodel = models[0];
												endDate = tmpmodel.attributes.value;
											} 
											XAUtils.displayDatepicker(that.ui.visualSearch, facet, endDate, callback);
											break;
									case 'End Date' :
										var startDate, models = that.visualSearch.searchQuery.where({category:"Start Date"});
										if(models.length > 0){
											var tmpmodel = models[0];
											startDate = tmpmodel.attributes.value;
										} 
										XAUtils.displayDatepicker(that.ui.visualSearch, facet, startDate, callback);
										break;
									case 'Today'	:
										var today = Globalize.format(new Date(),"yyyy/mm/dd");
										callback([today]);
										break;
								}     
			            	
							}
				      }
				};
			this.visualSearch = XAUtils.addVisualSearch(searchOpt,serverAttrName, this.authSessionList,pluginAttr);
		},
		addSearchForAgentTab : function(){
			var that = this;
			var searchOpt = ["Export Date", "Service Name", "Plugin Id", "Plugin IP", "Http Response Code"];
			searchOpt = _.without(searchOpt,'Export Date');
			searchOpt = _.union(searchOpt, ['Start Date','End Date']);//'Today'
			var serverAttrName  = [{text : "Plugin Id", label :"agentId"}, {text : "Plugin IP", label :"clientIP"},
			                       {text : "Service Name", label :"repositoryName"},{text : "Http Response Code", label :"httpRetCode"},
			                       {text : "Export Date", label :"createDate"},
			                       {text : 'Start Date',label :'startDate'},{text : 'End Date',label :'endDate'},
				                   {text : 'Today',label :'today'}];
			var pluginAttr = {
				      placeholder :localization.tt('h.searchForYourAgent'),
				      container : this.ui.visualSearch,
				      query     : '',
				      callbacks :  { 
				    	  valueMatches :function(facet, searchTerm, callback) {
								switch (facet) {
									case 'Audit Type':
										callback([]);
										break;
									case 'Start Date' :
											var endDate, models = that.visualSearch.searchQuery.where({category:"End Date"});
											if(models.length > 0){
												var tmpmodel = models[0];
												endDate = tmpmodel.attributes.value;
											} 
											XAUtils.displayDatepicker(that.ui.visualSearch, facet, endDate, callback);
											break;
									case 'End Date' :
										var startDate, models = that.visualSearch.searchQuery.where({category:"Start Date"});
										if(models.length > 0){
											var tmpmodel = models[0];
											startDate = tmpmodel.attributes.value;
										} 
										XAUtils.displayDatepicker(that.ui.visualSearch, facet, startDate, callback);
										break;
									case 'Today'	:
										var today = Globalize.format(new Date(),"yyyy/mm/dd");
										callback([today]);
										break;
								}     
			            	
							}
				      }
				};
			this.visualSearch = XAUtils.addVisualSearch(searchOpt,serverAttrName, this.policyExportAuditList, pluginAttr);
		},
		renderAdminTable : function(){
			var that = this , self = this;
			
			var TableRow = Backgrid.Row.extend({
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
					var self = this;
					if($(e.target).is('.icon-edit,.icon-trash,a,code'))
						return;
					this.$el.parent('tbody').find('tr').removeClass('tr-active');
					this.$el.toggleClass('tr-active');
					
					XAUtils.blockUI();
					var action = self.model.get('action');
					var date = new Date(self.model.get('createDate')).toString();
					var timezone = date.replace(/^.*GMT.*\(/, "").replace(/\)$/, "");
					var objectCreatedDate =  Globalize.format(new Date(self.model.get('createDate')), "MM/dd/yyyy hh:mm:ss tt")+' '+timezone;
					
					var fullTrxLogListForTrxId = new VXTrxLogList();
					fullTrxLogListForTrxId.getFullTrxLogListForTrxId(this.model.get('transactionId'),{
						cache : false
					}).done(function(coll,mm){
						XAUtils.blockUI('unblock');
						fullTrxLogListForTrxId = new VXTrxLogList(coll.vXTrxLogs);
						
						//diff view to support new plugable service model
						if(self.model.get('objectClassType') == XAEnums.ClassTypes.CLASS_TYPE_RANGER_POLICY.value){
							var view = new vPlugableServiceDiffDetail({
								collection : fullTrxLogListForTrxId,
								classType : self.model.get('objectClassType'),
								objectName : self.model.get('objectName'),
								objectId   : self.model.get('objectId'),
								objectCreatedDate : objectCreatedDate,
								userName :self.model.get('owner'),
								action : action
							});
						} else {
							var view = new vOperationDiffDetail({
								collection : fullTrxLogListForTrxId,
								classType : self.model.get('objectClassType'),
								objectName : self.model.get('objectName'),
								objectId   : self.model.get('objectId'),
								objectCreatedDate : objectCreatedDate,
								userName :self.model.get('owner'),
								action : action
							});
						}
						var modal = new Backbone.BootstrapModal({
							animate : true, 
							content		: view,
							title: localization.tt("h.operationDiff")+' : '+action,
							okText :localization.tt("lbl.ok"),
							allowCancel : true,
							escape : true
						}).open();
						modal.$el.addClass('modal-diff').attr('tabindex',-1);
						modal.$el.find('.cancel').hide();
					});
				}
			});
			this.ui.tableList.addClass("clickable");
			this.rTableList.show(new XATableLayout({
				columns: this.getAdminTableColumns(),
				collection:this.trxLogList,
				includeFilter : false,
				gridOpts : {
					row : TableRow,
					header : XABackgrid,
					emptyText : 'No service found!!'
				}
			}));	
		},
		getAdminTableColumns : function(){
			var auditList = [];
			_.each(XAEnums.ClassTypes, function(obj){
				if((obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_ASSET.value) 
						|| (obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_RESOURCE.value)
						|| (obj.value == XAEnums.ClassTypes.CLASS_TYPE_RANGER_POLICY.value) 
						|| (obj.value == XAEnums.ClassTypes.CLASS_TYPE_RANGER_SERVICE.value)
						|| (obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_USER.value)
						|| (obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_GROUP.value))
				auditList.push({text :obj.label, id :obj.value});
			});
			var cols = {
				operation : {
					label : localization.tt("lbl.operation"),
					cell: "html",
					click : false,
					drag : false,
					sortable:false,
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							rawValue = model.get('objectClassType');
							var action = model.get('action'), name = _.escape(model.get('objectName')),
								label = XAUtils.enumValueToLabel(XAEnums.ClassTypes,rawValue), html = '';
							if(rawValue == XAEnums.ClassTypes.CLASS_TYPE_XA_ASSET.value || rawValue == XAEnums.ClassTypes.CLASS_TYPE_RANGER_SERVICE.value)
								html = 	'Service '+action+'d '+'<b>'+name+'</b>';
							if(rawValue == XAEnums.ClassTypes.CLASS_TYPE_XA_RESOURCE.value || rawValue == XAEnums.ClassTypes.CLASS_TYPE_RANGER_POLICY.value)
								html = 	'Policy '+action+'d '+'<b>'+name+'</b>';
							if(rawValue == XAEnums.ClassTypes.CLASS_TYPE_XA_USER.value)
								html = 	'User '+action+'d '+'<b>'+name+'</b>';
							if(rawValue == XAEnums.ClassTypes.CLASS_TYPE_XA_GROUP.value)
								html = 	'Group '+action+'d '+'<b>'+name+'</b>';
							if(rawValue  == XAEnums.ClassTypes.CLASS_TYPE_USER_PROFILE.value)
								html = 	'User profile '+action+'d '+'<b>'+name+'</b>';
							if(rawValue  == XAEnums.ClassTypes.CLASS_TYPE_PASSWORD_CHANGE.value)
								html = 	'User profile '+action+'d '+'<b>'+name+'</b>';
							return html;
						}
					})
				},
				objectClassType : {
					label : localization.tt("lbl.auditType"),
					cell: "string",
					click : false,
					drag : false,
					sortable:false,
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							var label = XAUtils.enumValueToLabel(XAEnums.ClassTypes,rawValue);
							return label;
						}
					})
				},
				owner : {
					label : localization.tt("lbl.user"),
					cell: "string",
					click : false,
					drag : false,
					sortable:false,
					editable:false
				},
				createDate : {
					label : localization.tt("lbl.date") + '  ( '+this.timezone+' )',
					cell: "String",
					click : false,
					drag : false,
					editable:false,
                    sortType: 'toggle',
                    direction: 'descending',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							return Globalize.format(new Date(model.get('createDate')),  "MM/dd/yyyy hh:mm:ss tt");
						}
					})
				},
				action : {
					label : localization.tt("lbl.actions"),
					cell: "html",
					click : false,
					drag : false,
					sortable:false,
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							var html = '';
							if(rawValue =='create'){
								html = 	'<label class="label label-success">'+rawValue+'</label>';
							} else if(rawValue == 'update'){
								html = 	'<label class="label label-yellow">'+rawValue+'</label>';
							}else if(rawValue == 'delete'){
								html = 	'<label class="label label-important">'+rawValue+'</label>';
							} else {
								html = 	'<label class="label">'+rawValue+'</label>';
							}

							return html;
						}
					})
				},
				sessionId : {
					label : localization.tt("lbl.sessionId"),
					cell: "uri",
					href: function(model){
						return '#!/reports/audit/loginSession/extSessionId/'+model.get('sessionId');
					},
					click : false,
					drag : false,
					sortable:false,
					editable:false
				}
			};
			return this.trxLogList.constructor.getTableCols(cols, this.trxLogList);
		},

		renderBigDataTable : function(){
			var that = this , self = this;
			
			var TableRow = Backgrid.Row.extend({
				events: {
					'click' : 'onClick'
				},
				initialize : function(){
					var that = this;
					var args = Array.prototype.slice.apply(arguments);
					Backgrid.Row.prototype.initialize.apply(this, args);
				},
				onClick: function (e) {
					var self = this;
					var policyId = this.model.get('policyId');
					if(policyId == -1){
						return;
					}
					var	serviceDef = that.serviceDefList.findWhere({'id':this.model.get('repoType')});
					if(_.isUndefined(serviceDef)){
						return ;
					}
					var eventTime = this.model.get('eventTime');

					var policy = new RangerPolicy({
						id: policyId
					});
					var policyVersionList = policy.fetchVersions();
					var view = new RangerPolicyRO({
						policy: policy,
						policyVersionList : policyVersionList,
						serviceDef: serviceDef,
						eventTime : eventTime
					});
					var modal = new Backbone.BootstrapModal({
						animate : true, 
						content		: view,
						title: localization.tt("h.policyDetails"),
						okText :localization.tt("lbl.ok"),
						allowCancel : false,
						escape : true
					}).open();
					var policyVerEl = modal.$el.find('.modal-footer').prepend('<div class="policyVer pull-left"></div>').find('.policyVer');
					policyVerEl.append('<i id="preVer" class="icon-chevron-left '+ ((policy.get('version')>1) ? 'active' : '') +'"></i><text>Version '+ policy.get('version') +'</text>').find('#preVer').click(function(e){
						view.previousVer(e);
					});
					var policyVerIndexAt = policyVersionList.indexOf(policy.get('version').toString());
					policyVerEl.append('<i id="nextVer" class="icon-chevron-right '+ (!_.isUndefined(policyVersionList[++policyVerIndexAt])? 'active' : '')+'"></i>').find('#nextVer').click(function(e){
						view.nextVer(e);
					});
				}
			});

			this.ui.tableList.removeClass("clickable");
			this.rTableList.show(new XATableLayout({
				columns: this.getColumns(),
				collection: this.accessAuditList,
				includeFilter : false,
				gridOpts : {
					row: TableRow,
					header : XABackgrid,
					emptyText : 'No Access Audit found!'
				}
			}));
		
		},

		getColumns : function(){
			var that = this;
			var cols = {
					policyId : {
						cell : "html",
						formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue, model) {
								if(rawValue == -1){
									return '--';
								}	
								var serviceDef = that.serviceDefList.findWhere({'id' : model.get('repoType')})
								if(_.isUndefined(serviceDef)){
									return rawValue;
								}
								var href = 'javascript:void(0)';
								return '<a href="'+href+'" title="'+rawValue+'">'+rawValue+'</a>';
							}
						}),
						label	: localization.tt("lbl.policyId"),
						editable: false,
						sortable : false
					},
					eventTime : {
						label : 'Event Time',
						cell: "String",
						click : false,
						drag : false,
						editable:false,
                        sortType: 'toggle',
                        direction: 'descending',
						formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue, model) {
								return Globalize.format(new Date(rawValue),  "MM/dd/yyyy hh:mm:ss tt");
							}
						})
					},
					requestUser : {
						label : 'User',
						cell: "String",
						click : false,
						drag : false,
						editable:false
					},
					repoName : {
						label : 'Name / Type',
						cell: "html",
						click : false,
						drag : false,
						sortable:false,
						editable:false,
						formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue, model) {
								return '<div title="'+rawValue+'">'+_.escape(rawValue)+'</div>\
								<div title="'+model.get('serviceType')+'" style="border-top: 1px solid #ddd;">'+_.escape(model.get('serviceType'))+'</div>';;
							}
						})
					},
					resourceType: {
						label : 'Name / Type',
						cell: "html",
						click: false,
						formatter: _.extend({},Backgrid.CellFormatter.prototype,{
							 fromRaw: function(rawValue,model) {
								 var resourcePath = _.isUndefined(model.get('resourcePath')) ? undefined : model.get('resourcePath');
								 var resourceType = _.isUndefined(model.get('resourceType')) ? undefined : model.get('resourceType');
								 if(resourcePath) {
								 return '<span title="'+resourcePath+'">'+resourcePath+'</span>\
									<div title="'+resourceType+'" style="border-top: 1px solid #ddd;">'+resourceType+'</div>';
								 }
							 }
						}),
						drag: false,
						sortable: false,
						editable: false,
					},
					accessType : {
						label : localization.tt("lbl.accessType"),
						cell: "String",
						click : false,
						drag : false,
						sortable:false,
						editable:false
					},
					accessResult : {
						label : localization.tt("lbl.result"),
						cell: "html",
						click : false,
						drag : false,
						editable:false,
						formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue) {
								var label = '', html = '';
								_.each(_.toArray(XAEnums.AccessResult),function(m){
									if(parseInt(rawValue) == m.value){
										label=  m.label;
										if(m.value == XAEnums.AccessResult.ACCESS_RESULT_ALLOWED.value){
											html = 	'<label class="label label-success">'+label+'</label>';
										} else {
											html = 	'<label class="label label-important">'+label+'</label>';
										} 
									}	
								});
								return html;
							}
						})
					},
					aclEnforcer : {
						label :localization.tt("lbl.aclEnforcer"),
						cell: "String",
						click : false,
						drag : false,
						sortable:false,
						editable:false
					},
					clientIP : {
						label : 'Client IP',
						cell: "string",
						click : false,
						drag : false,
						sortable:false,
						editable:false
					},
					eventCount : {
						label : 'Event Count',
						cell: "string",
						click : false,
						drag : false,
						sortable:false,
						editable:false
					},
					tags : {
						label : 'Tags',
						cell: "string",
						click : false,
						drag : false,
						sortable:false,
						editable:false
					},
			};
			return this.accessAuditList.constructor.getTableCols(cols, this.accessAuditList);
		},
		renderLoginSessionTable : function(){
			var that = this;
			this.ui.tableList.removeClass("clickable");
			this.rTableList.show(new XATableLayout({
				columns: this.getLoginSessionColumns(),
				collection:this.authSessionList,
				includeFilter : false,
				gridOpts : {
					row :Backgrid.Row.extend({}),
					header : XABackgrid,
					emptyText : 'No login session found!!'
				}
			}));	
		},
		getLoginSessionColumns : function(){
			var authStatusList = [],authTypeList = [];
			_.each(XAEnums.AuthStatus, function(obj){
					if(obj.value !=  XAEnums.AuthStatus.AUTH_STATUS_UNKNOWN.value)
						authStatusList.push({text :obj.label, id :obj.value});
			});
			_.each(XAEnums.AuthType, function(obj){
				if(obj.value !=  XAEnums.AuthType.AUTH_TYPE_UNKNOWN.value)
					authTypeList.push({text :obj.label, id :obj.value});
			});

			var cols = {
				id : {
					label : localization.tt("lbl.sessionId"),
					cell : "uri",
					href: function(model){
						return '#!/reports/audit/loginSession/id/'+model.get('id');
					},
					editable:false,
					sortType: 'toggle',
					direction: 'descending'
				},
				loginId : {
					label : localization.tt("lbl.loginId"),
					cell: "String",
					sortable:false,
					editable:false
				},
				authStatus : {
					label : localization.tt("lbl.result"),
					cell :"html",
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							var label = '', html = '';
							_.each(_.toArray(XAEnums.AuthStatus),function(m){
								if(parseInt(rawValue) == m.value){
									label=  m.label;
									if(m.value == 1){
										html = 	'<label class="label label-success">'+label+'</label>';
									} else if(m.value == 2){
										html = 	'<label class="label label-important">'+label+'</label>';
									} else {
										html = 	'<label class="label">'+label+'</label>';
									}
								}	
							});
							return html;
						}
					}),
					sortable:false,
					editable:false
				},
				authType: {
					label : localization.tt("lbl.loginType"),
					cell :"string",
					sortable:false,
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							var label='';
							_.each(_.toArray(XAEnums.AuthType),function(m){
								if(parseInt(rawValue) == m.value){
									label=  m.label;
								}	
							});
							return label;
						}
					})
					
				},
				requestIP : {
					label : localization.tt("lbl.ip"),
					cell: "String",
					sortable:false,
					editable:false,
					placeholder : localization.tt("lbl.ip")
				},
				requestUserAgent : {
					label : localization.tt("lbl.userAgent"),
					cell: "html",
					sortable:false,
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue,model) {
							return _.isUndefined(rawValue) ? '--': 
								'<span title="'+_.escape(rawValue) +'" class="showMore">'+_.escape(rawValue)+'</span>';
						}
					})
				},
				authTime : {
					label : localization.tt("lbl.loginTime")+ '   ( '+this.timezone+' )',
					cell: "String",
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							return Globalize.format(new Date(model.get('authTime')),  "MM/dd/yyyy hh:mm:ss tt");
						}
					})
				}
			};
			return this.authSessionList.constructor.getTableCols(cols, this.authSessionList);
		},
		renderAgentTable : function(){
			this.ui.tableList.removeClass("clickable");
			this.rTableList.show(new XATableLayout({
				columns: this.getAgentColumns(),
				collection: this.policyExportAuditList,
				includeFilter : false,
				gridOpts : {
					row : 	Backgrid.Row.extend({}),
					header : XABackgrid,
					emptyText : 'No plugin found!'
				}
			}));	
		},
		getAgentColumns : function(){
			var cols = {
					createDate : {
						cell : 'string',
						formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue, model) {
								return Globalize.format(new Date(model.get('createDate')),  "MM/dd/yyyy hh:mm:ss tt");
							}
						}),
						label : localization.tt('lbl.createDate')+ '   ( '+this.timezone+' )',
						editable:false,
						sortType: 'toggle',
						direction: 'descending'
					},
					repositoryName : {
						cell : 'html',
						label	: localization.tt('lbl.serviceName'),
						editable:false,
						sortable:false,
						formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue, model) {
								rawValue = _.escape(rawValue);
								return '<span title="'+rawValue+'">'+rawValue+'</span>';
							}
						}),
						
					},
					agentId : {
						cell : 'string',
						label	:localization.tt('lbl.agentId'),
						editable:false,
						sortable:false
					},
					clientIP : {
						cell : 'string',
						label	: localization.tt('lbl.agentIp'),
						editable:false,
						sortable:false
					},
					httpRetCode : {
						cell : 'html',
						label	: localization.tt('lbl.httpResponseCode'),
						editable:false,
						sortable:false,
						formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue, model) {
								var html = rawValue;
								return (rawValue > 400) ? '<label class="label btn-danger">'+rawValue+'</label>'
										:	'<label class="label btn-success">'+rawValue+'</label>';
							}
						})
					},
					syncStatus : {
						cell : 'string',
						label	: 'Status',
						editable:false,
						sortable:false
					},
			};
			return this.policyExportAuditList.constructor.getTableCols(cols, this.policyExportAuditList);
		},
		onRefresh : function(){
			var that =this, coll,params = {};
			var lastUpdateTime = Globalize.format(new Date(),  "MM/dd/yyyy hh:mm:ss tt");
			that.ui.lastUpdateTimeLabel.html(lastUpdateTime);
			switch (this.currentTab) {
			case "#bigData":
				coll = this.accessAuditList;
				break;
			case "#admin":
				coll = this.trxLogList;
				break;
			case "#loginSession":
				coll = this.authSessionList;
				break;
			case "#agent":
				//TODO
				params = { 'priAcctId' : 1 };
				coll = this.policyExportAuditList;
				break;	
			}
			coll.fetch({
				reset : true,
				data  : params,
				success: function(){
					that.ui.lastUpdateTimeLabel.html(lastUpdateTime);
				},
				error : function(){
					that.ui.lastUpdateTimeLabel.html(lastUpdateTime);
				}
				
			});
		},
		onSearch : function(e){
			this.setCurrentTabCollection();
			var resourceName= this.ui.resourceName.val(),
				startDate 	= this.ui.startDate.val(),
				endDate  	= this.ui.endDate.val(),
				params = { 'startDate' : startDate , 'endDate' : endDate };
			//After every search we need goto to first page
			$.extend(this.collection.queryParams,params);
			//collection.fetch({data: data, reset: true});

			this.collection.state.currentPage = this.collection.state.firstPage;
			this.collection.fetch({
				reset : true,
				cache : false,
			});
		},
		setCurrentTabCollection : function(){
			switch (this.currentTab) {
			case "#admin":
				this.collection = this.trxLogList;
				break;
			case "#bigData":
				this.collection = this.accessAuditList;
				break;
			case "#loginSession":
				this.collection = this.authSessionList;
				break;
			case "#agent":
				this.collection = this.policyExportAuditList;
				break;
			}
		},
		clearVisualSearch : function(collection, serverAttrNameList) {
			_.each(serverAttrNameList, function(obj) {
				if (!_.isUndefined(collection.queryParams[obj.label]))
					delete collection.queryParams[obj.label];
			});
		},

		/** all post render plugin initialization */
		initializePlugins : function() {
		},
		updateLastRefresh : function(){
			this.ui.lastUpdateTimeLabel.html(Globalize.format(new Date(),  "MM/dd/yyyy hh:mm:ss tt"));
		},
		/** on close */
		onClose : function() {
			clearInterval(this.timerId);
			clearInterval(this.clearTimeUpdateInterval);
			$('.datepicker').remove();
		}
	});

	return AuditLayout;
});
