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
	
	var VXAuthSession			= require('collections/VXAuthSessionList');
	var VXTrxLogList   			= require('collections/VXTrxLogList');
	var VXAssetList 			= require('collections/VXAssetList');
	var VXPolicyExportAuditList = require('collections/VXPolicyExportAuditList');
	var AuditlayoutTmpl 		= require('hbs!tmpl/reports/AuditLayout_tmpl');
	var vOperationDiffDetail	= require('views/reports/OperationDiffDetail');

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
			//'rAuditTable'	: 'div[data-id="r_auditTable"]',
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
			//events['blur ' + this.ui.resourceName ]  = 'onSearch';
			//events['change ' + this.ui.selectRepo ]  = 'onChangeRepository';
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
		//	this.initializePolling();
			this.bindEvents();
			this.currentTab = '#'+this.tab;
			var date = new Date().toString();
			this.timezone = date.replace(/^.*GMT.*\(/, "").replace(/\)$/, "");
		},

		/** all events binding here */
		bindEvents : function() {
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
			//this.listenTo(this.collection, "change:foo", this.render, this);
		},
		initializeCollection : function(){
			
			this.collection.fetch({
				reset : true,
				cache : false
				//data : params,
			});
		},
		initializePolling : function(){
			this.timerId = setInterval(function(){
//				that.onRefresh(this.accessAuditList);
				console.log('polling collection..');
				//that.initializeCollection();
				//that.onSearch();
			},XAGlobals.settings.AUDIT_REPORT_POLLING);
		},
		/** on render callback */
		onRender : function() {
			this.initializePlugins();
			if(this.currentTab != '#bigData'){
				this.onTabChange();
				this.ui.tab.find('li[class="active"]').removeClass();
				this.ui.tab.find('[href="'+this.currentTab+'"]').parent().addClass('active');
				
			}else{
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
					<th class="renderable ruser"></th>\
					<th class="renderable ruser"></th>\
					<th class="renderable cip">Repository</th>\
					<th class="renderable name"  ></th>\
					<th class="renderable cip"></th>\
					<th class="renderable cip"></th>\
					<th class="renderable cip"> </th>\
					<th class="renderable aip" > </th>\
				</tr>');
		},
		renderDateFields : function(){
			var that = this;
			this.ui.startDate.datepicker({
				autoclose : true
				//language : $.cookie().lang
			}).on('changeDate', function(ev) {
				that.ui.endDate.datepicker('setStartDate', ev.date);
			}).on('keydown',function(){
				return false;
			});
			this.ui.endDate.datepicker({
				autoclose : true
				//language : $.cookie().lang
			}).on('changeDate', function(ev) {
				that.ui.startDate.datepicker('setEndDate', ev.date);
			}).on('keydown',function(){
				return false;
			});
		},
		onTabChange : function(e){
			var that = this, tab;
			if(!_.isUndefined(e))
				tab = $(e.currentTarget).attr('href');
				//tab = $(e.currentTarget).find('a').attr('href');
			else
				tab = this.currentTab;
			this.$el.parents('body').find('.datepicker').remove();
			switch (tab) {
			case "#bigData":
				this.currentTab = '#bigData';
				this.ui.visualSearch.show();
				this.ui.visualSearch.parents('.well').show();
//				this.accessAuditList = new VXAccessAuditList();
				this.renderBigDataTable();
				this.modifyTableForSubcolumns();
				if(this.accessAuditList.length <= 0){
					this.accessAuditList.fetch({
						cache : false
					});
				}
				this.addSearchForBigDataTab();
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
					
				break;
			case "#loginSession":
				this.currentTab = '#loginSession';
				this.authSessionList = new VXAuthSession();
				this.renderLoginSessionTable();
				//var params = { sortBy : 'id',sortType: 'desc' };
				//Setting SortBy as id and sortType as desc = 1
				this.authSessionList.setSorting('id',1); 
				this.authSessionList.fetch({
					cache:false,
				//	data:params
				});
				this.addSearchForLoginSessionTab();
				break;
			case "#agent":
				this.currentTab = '#agent';
				this.policyExportAuditList = new VXPolicyExportAuditList();	
				var params = { priAcctId : 1 };
				that.renderAgentTable();
				this.policyExportAuditList.fetch({
					cache : false,
					data :params
				});
				this.addSearchForAgentTab();
				break;	
			}
			var lastUpdateTime = Globalize.format(new Date(),  "MM/dd/yyyy hh:mm:ss tt");
			that.ui.lastUpdateTimeLabel.html(lastUpdateTime);
		},
		addSearchForBigDataTab :function(){
			var that = this;
			
			var serverAttrName = [{text : 'Start Date',label :'startDate'},{text : 'End Date',label :'endDate'},
			                      {text : 'Today',label :'today'},{text : 'User',label :'requestUser'},
			                      {text : 'Resource Name',label :'resourcePath'},{text : 'Policy ID',label :'policyId'},
			                      {text : 'Resource Type',label :'resourceType'},{text : 'Repository Name',label :'repoName'},
			                      {text : 'Repository Type',label :'repoType','multiple' : true, 'optionsArr' : XAUtils.enumToSelectLabelValuePairs(XAEnums.AssetType)},
			                      {text : 'Result',label :'accessResult', 'multiple' : true, 'optionsArr' : XAUtils.enumToSelectLabelValuePairs(XAEnums.AccessResult)},
			                      {text : 'Access Type',label :'accessType'},{text : 'Access Enforcer',label :'aclEnforcer'},
			                      {text : 'Audit Type',label :'auditType'},{text : 'Session ID',label :'sessionId'},
			                      {text : 'Client IP',label :'clientIP'},{text : 'Client Type',label :'clientType'}];
            var searchOpt = ['Start Date','End Date','User','Repository Name','Repository Type','Resource Name','Access Type','Result','Access Enforcer','Client IP'];//,'Policy ID'
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
							if((obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_ASSET.value) ||
									(obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_RESOURCE.value) ||
									(obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_USER.value) || 
									(obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_GROUP.value))
								auditList.push({label :obj.label, value :obj.value+''});
						});
						
						switch (facet) {
							case 'Repository Name':
								var assetList 	= new VXAssetList();
								assetList.fetch().done(function(){
									callback(assetList.map(function(model){return model.get('name');}));
								});
								break;
							case 'Repository Type':
								var assetTypeList = _.filter(XAEnums.AssetType, function(obj){
									if(obj.label != XAEnums.AssetType.ASSET_UNKNOWN.label)
										return obj;
								});
								callback(XAUtils.hackForVSLabelValuePairs(assetTypeList));
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
			//var searchOpt = _.pluck(this.getAdminTableColumns(), 'label');
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
				if((obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_ASSET.value) ||
						(obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_RESOURCE.value) ||
						(obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_USER.value) || 
						(obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_GROUP.value))
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
			//var searchOpt = _.pluck(this.getLoginSessionColumns(), 'label');
			var searchOpt = ["Session Id", "Login Id", "Result", "Login Type", "IP", "User Agent", "Login Time"];
			searchOpt = _.without(searchOpt,'Login Time');
			//var searchOpt = ["Session Id", "Login Id", "Result", "Login Type", "IP", "User Agent", "Login Time"]
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
			//var searchOpt = _.pluck(this.getAgentColumns(), 'label');
			var searchOpt = ["Export Date", "Repository Name", "Agent Id", "Agent IP", "Http Response Code"];
			searchOpt = _.without(searchOpt,'Export Date');
			searchOpt = _.union(searchOpt, ['Start Date','End Date']);//'Today'
			var serverAttrName  = [{text : "Agent Id", label :"agentId"}, {text : "Agent IP", label :"clientIP"},
			                       {text : "Repository Name", label :"repositoryName"},{text : "Http Response Code", label :"httpRetCode"},
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
						
						var view = new vOperationDiffDetail({
							collection : fullTrxLogListForTrxId,
							classType : self.model.get('objectClassType'),
							objectName : self.model.get('objectName'),
							objectId   : self.model.get('objectId'),
							objectCreatedDate : objectCreatedDate,
							userName :self.model.get('owner'),
							action : action
							
						});
						var modal = new Backbone.BootstrapModal({
							animate : true, 
							content		: view,
							title: localization.tt("h.operationDiff")+' : '+action,
							//	cancelText : localization.tt("lbl.done"),
							okText :localization.tt("lbl.ok"),
							allowCancel : true,
							escape : true
						}).open();
						modal.$el.addClass('modal-diff').attr('tabindex',-1);
						modal.$el.find('.cancel').hide();
					});
					
					
					
				}
			});
//			this.ui.refreshTable.hide();
			this.ui.tableList.addClass("clickable");
			this.rTableList.show(new XATableLayout({
				columns: this.getAdminTableColumns(),
				collection:this.trxLogList,
				includeFilter : false,
				gridOpts : {
					row : TableRow,
					header : XABackgrid,
					emptyText : 'No repository found!!'
				}
			}));	
		},
		getAdminTableColumns : function(){
			var auditList = [];
			_.each(XAEnums.ClassTypes, function(obj){
								if((obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_ASSET.value) ||
										(obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_RESOURCE.value) ||
										(obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_USER.value) || 
										(obj.value == XAEnums.ClassTypes.CLASS_TYPE_XA_GROUP.value))
									auditList.push({text :obj.label, id :obj.value});
							});
			console.log(auditList);
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
							var action = model.get('action');
							var name = model.get('objectName');
							var html = '';
							var label = XAUtils.enumValueToLabel(XAEnums.ClassTypes,rawValue);
							if(rawValue == XAEnums.ClassTypes.CLASS_TYPE_XA_ASSET.value)
								html = 	'Repository '+action+'d '+'<b>'+name+'</b>';//'<a tabindex="-1" href="javascript:;" title="'+name+'">'+name+'</a>';
							if(rawValue == XAEnums.ClassTypes.CLASS_TYPE_XA_RESOURCE.value)
								html = 	'Policy '+action+'d '+'<b>'+name+'</b>';//'<a tabindex="-1" href="javascript:;" title="'+name+'">'+name+'</a>';
							if(rawValue == XAEnums.ClassTypes.CLASS_TYPE_XA_USER.value)
								html = 	'User '+action+'d '+'<b>'+name+'</b>';//'<a tabindex="-1" href="javascript:;" title="'+name+'">'+name+'</a>';
							if(rawValue == XAEnums.ClassTypes.CLASS_TYPE_XA_GROUP.value)
								html = 	'Group '+action+'d '+'<b>'+name+'</b>';//'<a tabindex="-1" href="javascript:;" title="'+name+'">'+name+'</a>';
							if(rawValue  == XAEnums.ClassTypes.CLASS_TYPE_USER_PROFILE.value)
								html = 	'User profile '+action+'d '+'<b>'+name+'</b>';//'<a tabindex="-1" href="javascript:;" title="'+name+'">'+name+'</a>';
							if(rawValue  == XAEnums.ClassTypes.CLASS_TYPE_PASSWORD_CHANGE.value)
								html = 	'User profile '+action+'d '+'<b>'+name+'</b>';//'<a tabindex="-1" href="javascript:;" title="'+name+'">'+name+'</a>';
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
					//sortable:false,
					editable:false,
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
							if(rawValue =='create')
								html = 	'<label class="label label-success">'+rawValue+'</label>';
							else if(rawValue == 'update')
								html = 	'<label class="label label-yellow">'+rawValue+'</label>';
							else if(rawValue == 'delete')
								html = 	'<label class="label label-important">'+rawValue+'</label>';
							else
								html = 	'<label class="label">'+rawValue+'</label>';
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
			
			this.ui.tableList.removeClass("clickable");
			this.rTableList.show(new XATableLayout({
				columns: this.getColumns(),
				collection: this.accessAuditList,
				includeFilter : false,
				gridOpts : {
					row: Backgrid.Row.extend({}),
					header : XABackgrid,
					emptyText : 'No Access Audit found!'
				}
			}));
		
		},

		getColumns : function(){
			var that = this;
			var cols = {
					eventTime : {
						label : 'Event Time',// localization.tt("lbl.eventTime"),
						cell: "String",
						click : false,
						drag : false,
						editable:false,
						formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue, model) {
								return Globalize.format(new Date(rawValue),  "MM/dd/yyyy hh:mm:ss tt");
							}
						})
					},
					requestUser : {
						label : 'User',//localization.tt("lbl.repositoryName"),
						cell: "String",
						click : false,
						drag : false,
					//	sortable:false,
						editable:false
					},
					repoName : {
						label : 'Name / Type',//localization.tt("lbl.repositoryName"),
						cell: "html",
						click : false,
						drag : false,
						sortable:false,
						editable:false,
						formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue, model) {
								var html='';
								var repoType = model.get('repoType');
								_.each(_.toArray(XAEnums.AssetType),function(m){
									if(parseInt(repoType) == m.value){
										html =  '<div title="'+rawValue+'">'+rawValue+'</div>\
										<div title="'+rawValue+'" style="border-top: 1px solid #ddd;">'+m.label+'</div>';
										return ;
									}	
								});
								return html;
							}
						})
					},
					/*repoType : {
						label : '',//localization.tt("lbl.repoType"),
						cell: "html",
						click : false,
						drag : false,
						sortable:false,
						editable:false,
						formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue) {
								var html='';
								_.each(_.toArray(XAEnums.AssetType),function(m){
									if(parseInt(rawValue) == m.value){
										html = 	'<label class="label label-info">'+m.label+'</label>';
									}	
								});
								return rawValue;
							}
						}),
					},*/
					/*policyType : {
							label : '',//localization.tt("lbl.resourceType"),
							cell: "String",
							click : false,
							drag : false,
							sortable:false,
							editable:false,
						},*/
					resourcePath : {
						label : localization.tt("lbl.resourceName"),
						cell: "html",
						click : false,
						drag : false,
						sortable:false,
						editable:false,
						formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue) {
								return _.isUndefined(rawValue) ? '--': '<span title="'+rawValue+'">'+rawValue+'</span>';  
							}
						})
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
			//			sortable:false,
						editable:false,
						formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue) {
								var label = '', html = '';
								_.each(_.toArray(XAEnums.AccessResult),function(m){
									if(parseInt(rawValue) == m.value){
										label=  m.label;
										if(m.value == XAEnums.AccessResult.ACCESS_RESULT_ALLOWED.value)
											html = 	'<label class="label label-success">'+label+'</label>';
										else 
											html = 	'<label class="label label-important">'+label+'</label>';
										//else
										//	html = 	'<label class="label">'+label+'</label>';
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
					/*policyId : {
						label : 'Policy ID',//localization.tt("lbl.resourceId"),
						cell: "string",
						click : false,
						drag : false,
			//			sortable:false,
						editable:false
						formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue, model) {
								return '<div>'+rawValue+'</div>\
								<div style="border-top: 1px solid #ddd;">'+model.get('resourceType')+'</div>';
							}
						}),
					},*/
					clientIP : {
						label : 'Client IP',//localization.tt("lbl.ip"),
						cell: "string",
						click : false,
						drag : false,
						sortable:false,
						editable:false
						/*formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue, model) {
								return '<div>'+rawValue+'</div>\
								<div style="border-top: 1px solid #ddd;">'+model.get('clientType')+'</div>';
							}
						}),*/
					}
				/*agentId : {
					label : 'ID IP',//localization.tt("lbl.id"),
					cell: "html",
					click : false,
					drag : false,
					sortable:false,
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							return '<div>'+rawValue+'</div>\
							<div style="border-top: 1px solid #ddd;">'+model.get('agentIp')+'</div>';
						}
					}),
					
				},*/
				/*agentIp : {
					label : localization.tt("lbl.ip"),
					cell: "String",
					click : false,
					drag : false,
					sortable:false,
					editable:false,
				},*/
			/*	clientId : {
					label : 'ID Type',//localization.tt("lbl.clientId"),
					cell: "html",
					click : false,
					drag : false,
					sortable:false,
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							return '<div>'+rawValue+'</div>\
							<div style="border-top: 1px solid #ddd;">'+model.get('clientType')+'</div>';
						}
					}),
				},*/
				/*clientType : {
					label : '',//localization.tt("lbl.clientType"),
					cell: "String",
					click : false,
					drag : false,
					sortable:false,
					editable:false,
				},*/
			/*	auditType : {
					label : '',//localization.tt("lbl.auditType"),
					cell: "html",
					click : false,
					drag : false,
					sortable:false,
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							var html='';
							_.each(_.toArray(XAEnums.XAAuditType),function(m){
								if(parseInt(rawValue) == m.value){
									html = 	'<label class="label label-success">'+m.label+'</label>';
								}	
							});
							return html;
						}
					}),
				},
				sessionId : {
					label : '',//localization.tt("lbl.sessionId"),
					cell: "String",
					click : false,
					drag : false,
					sortable:false,
					editable:false,
				},*/
				
			};
			return this.accessAuditList.constructor.getTableCols(cols, this.accessAuditList);
		},
		renderLoginSessionTable : function(){
			var that = this;
			this.ui.tableList.removeClass("clickable");
			
			/*var TableRow = Backgrid.Row.extend({
				events: {
					'click' : 'onClick',
					'mouseenter' : 'onClick',
					'mouseleave' : 'onMouseLeave'
				},
				initialize : function(){
					var that = this;
					var args = Array.prototype.slice.apply(arguments);
					Backgrid.Row.prototype.initialize.apply(this, args);
				},
				onClick: function (e,s,a) {
					var self = this;
					if($(e.target).is('.icon-edit,.icon-trash,a,code'))
						return;
					this.$('[data-id="'+this.model.id+'"]').popover('show');
					if(!$('.popover').is('div'))
						this.$('[data-id="'+this.model.id+'"]').popover('show');
					else
						this.$('[data-id="'+this.model.id+'"]').popover('hide');
				//	this.$el.parent('tbody').find('tr').removeClass('tr-active');
				//	this.$el.toggleClass('tr-active');
					if($(e.target).has('showMore')){
						if($(e.target).is('span') && $(e.target).text() != '--'){
							var text = $(e.target).text();
							var popoverHTML = '<div type="button" class="btn btn-default" data-container="body" data-toggle="popover" data-placement="right" data-content="'+text+'">\
							  Popover on right\
							</div>';
							$(e.target).parent().append(popoverHTML);
							$('[data-toggle="popover"]').popover({
								  trigger: 'hover'
							});
							
						}else if($(e.target).is('td') && $(e.target).text() != '--'){
							var text = $(e.target).find('span').text();
							$(e.target).find('span').css('position','relative');
							tooltip.setAttribute( "class", "tooltips" );
							tooltip.innerText = $(e.target).find('span').attr("title");
							//tooltip.style.display = "inline";
							$(tooltip).attr('style','position:absolute;display:block;').css("top", Y).css("left", X);
							$(e.target).append(tooltip);
						}
					}
				},
				onMouseLeave : function(e){
					var self = this;
				//	this.$('[data-toggle="popover"]').popover('hide');
//					this.$('[data-id="'+this.model.id+'"]').popover('hide');
					if($(e.target).is('.icon-edit,.icon-trash,a,code'))
						return;
				//	this.$el.parent('tbody').find('tr').removeClass('tr-active');
				//	this.$el.toggleClass('tr-active');
				//	if($(e.target).has('td'))
				//		$('.tooltips').remove();
				}
			});
			*/
			this.rTableList.show(new XATableLayout({
				columns: this.getLoginSessionColumns(),
				collection:this.authSessionList,
				includeFilter : false,
				gridOpts : {
//					row :TableRow,//Backgrid.Row.extend({}),
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
			console.log(authStatusList);
			var cols = {
				id : {
					label : localization.tt("lbl.sessionId"),
					cell : "uri",
					href: function(model){
						return '#!/reports/audit/loginSession/id/'+model.get('id');
					},
//					sortable:false,
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
									if(m.value == 1)
										html = 	'<label class="label label-success">'+label+'</label>';
									else if(m.value == 2)
										html = 	'<label class="label label-important">'+label+'</label>';
									else
										html = 	'<label class="label">'+label+'</label>';
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
							//return '<label class="label label-info">'+label+'</label>';
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
								/*'<div data-id="'+model.id+'" data-container="body" data-toggle="popover" data-placement="right" data-content="'+rawValue+'" style="cursor:pointer;">\
									'+rawValue+'</div>';*/
							'<span title="'+rawValue
							+'" class="showMore">'+rawValue+'</span>';
						}
					})
				},
				authTime : {
					label : localization.tt("lbl.loginTime")+ '   ( '+this.timezone+' )',
					cell: "String",
				//	sortable:false,
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
					emptyText : 'No agent found!'
				}
			}));	
		},
		getAgentColumns : function(){
			var cols = {
					createDate : {
						cell : 'string',
						formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue, model) {
								//var date = new Date(model.get('createDate')).toString();
								//var timezone = date.replace(/^.*GMT.*\(/, "").replace(/\)$/, "");
								return Globalize.format(new Date(model.get('createDate')),  "MM/dd/yyyy hh:mm:ss tt");
							}
						}),
						label : localization.tt('lbl.createDate')+ '   ( '+this.timezone+' )',
						editable : false
					},
					repositoryName : {
						cell : 'html',
						label	: localization.tt('lbl.repositoryName'),
						editable:false,
						sortable:false,
						formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
							fromRaw: function (rawValue, model) {
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
								if(rawValue > 400)
									html = '<label class="label label-yellow">'+rawValue+'</label>';
								else
									html = '<label class="label">'+rawValue+'</label>';
								return html;
							}
						})
					}
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
			//var param = {};
			this.setCurrentTabCollection();
			var resourceName= this.ui.resourceName.val();
			//var assetType 	= this.ui.selectRepo.select2('val');
			var startDate 	= this.ui.startDate.val();
			var endDate  	= this.ui.endDate.val();
			var params = { 'startDate' : startDate , 'endDate' : endDate };
			//After every search we need goto to first page
			$.extend(this.collection.queryParams,params);
			//collection.fetch({data: data, reset: true});

			this.collection.state.currentPage = this.collection.state.firstPage;
			
			this.collection.fetch({
				reset : true,
				cache : false
				//data : params,
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

		/*
		 * onChangeRepository : function(){ this.onSearch(); },
		 */
		/** all post render plugin initialization */
		initializePlugins : function() {
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
