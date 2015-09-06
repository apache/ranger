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

 
define(function(require) {'use strict';

	var Backbone 			= require('backbone');
	var XALinks 			= require('modules/XALinks');
	var XAEnums 			= require('utils/XAEnums');
	var XAUtil				= require('utils/XAUtils');
	var XABackgrid			= require('views/common/XABackgrid');
	var XATableLayout		= require('views/common/XATableLayout');
	var localization		= require('utils/XALangSupport');
	
	var RangerService		= require('models/RangerService');
	var RangerServiceDefList= require('collections/RangerServiceDefList');
	var RangerPolicyList	= require('collections/RangerPolicyList');
	var UseraccesslayoutTmpl= require('hbs!tmpl/reports/UserAccessLayout_tmpl');

	var UserAccessLayout 	= Backbone.Marionette.Layout.extend(
	/** @lends UserAccessLayout */
	{
		_viewName : 'UserAccessLayout',

		template : UseraccesslayoutTmpl,
		breadCrumbs : [XALinks.get('UserAccessReport')],
		templateHelpers :function(){
			return {
				groupList : this.groupList,
				policyHeaderList : this.policyCollList
			};
		},

		/** Layout sub regions */
		regions :function(){
			var regions = {};
			this.initializeRequiredData();
			_.each(this.policyCollList, function(obj) {
				regions[obj.collName+'Table'] =  'div[data-id="'+obj.collName+'"]';
			},this)
			
			return regions;
		},

		/** ui selector cache */
		ui : {
			userGroup 			: '[data-js="selectGroups"]',
			searchBtn 			: '[data-js="searchBtn"]',
			userName 			: '[data-js="userName"]',
			resourceName 		: '[data-js="resourceName"]',
			policyName 			: '[data-js="policyName"]',
			gotoHive 			: '[data-js="gotoHive"]',
			gotoHbase 			: '[data-js="gotoHbase"]',
			gotoKnox 			: '[data-js="gotoKnox"]',
			gotoStorm 			: '[data-js="gotoStorm"]',
			btnShowMore 		: '[data-id="showMore"]',
			btnShowLess 		: '[data-id="showLess"]',
			btnShowMoreUsers 	: '[data-id="showMoreUsers"]',
			btnShowLessUsers 	: '[data-id="showLessUsers"]'
		},

		/** ui events hash */
		events : function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			events['click ' + this.ui.searchBtn]  = 'onSearch';
			//events["'change ' "+ this.ui.userName +"','" + this.ui.userGroup+"'"]  = 'onSearch';
			//events['change ' + this.ui.userName+ ','+this.ui.userGroup+ ',' +this.ui.resourceName] = 'onSearch';
			events['click .autoText']  = 'autocompleteFilter';
			
			//events['keydown ' + this.ui.userGroup]  = 'groupKeyDown';
			events['click .gotoLink']  = 'gotoTable';
			events['click ' + this.ui.btnShowMore]  = 'onShowMore';
			events['click ' + this.ui.btnShowLess]  = 'onShowLess';
			events['click ' + this.ui.btnShowMoreUsers]  = 'onShowMoreUsers';
			events['click ' + this.ui.btnShowLessUsers]  = 'onShowLessUsers';
			return events;
		},

		/**
		 * intialize a new UserAccessLayout Layout
		 * @constructs
		 */
		initialize : function(options) {
			console.log("initialized a UserAccessLayout Layout");
			_.extend(this, _.pick(options, 'groupList','userList'));
			this.bindEvents();
			
		},
		initializeRequiredData : function() {
			this.policyCollList = [];
			this.initializeServiceDef();
			this.serviceDefList.each(function(servDef) {
				var serviceDefName = servDef.get('name')
				var collName = serviceDefName +'PolicyList';
				this[collName] = new RangerPolicyList();
				this.defaultPageState = this[collName].state;
				this.policyCollList.push({ 'collName' : collName, 'serviceDefName' : serviceDefName})
			},this);
		},
		initializeServiceDef : function() {
			   this.serviceDefList = new RangerServiceDefList();
//			   this.serviceDefList.queryParams.sortBy = 'id';
			   this.serviceDefList.fetch({
				   cache : false,
				   async:false
			   });
		},	   

		/** all events binding here */
		bindEvents : function() {
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
//			this.listenTo(this.hiveResourceList, "change:foo", function(){alert();}, this);
		},

		onRender : function() {
			this.initializePlugins();
			this.setupGroupAutoComplete();
			//Show policies listing for each service and GET policies for each service
			_.each(this.policyCollList, function(obj,i){
				this.renderTable(obj.collName, obj.serviceDefName);
				this.getResourceLists(obj.collName,obj.serviceDefName);
			},this);
		},
		
		getResourceLists: function(collName, serviceDefName){
			var that = this, coll = this[collName];
			coll.queryParams.serviceType = serviceDefName;
		/*	if(!_.isUndefined(params)){
				_.each(params,function(val, attr){ 
					if(!_.isUndefined(val) && !_.isEmpty(val)) 
						coll.queryParams[attr] = val;
				});
			}*/
			coll.fetch({
				cache : false,
				reset: true,
				async:false,
			}).done(function(){
//				console.log(coll);
				coll.trigger('sync')
//				if(coll.queryParams.assetType == XAEnums.AssetType.ASSET_STORM.value){
//					var totalRecords=0; 
//					_.each(that.resourceList, function( list ){ totalRecords += list.state.totalRecords; });
					/*that.$('[data-js="searchResult"]').html('Total '+totalRecords+' records found.');
					that.$('[data-js="hdfsSearchResult"]').html(that.hdfsResourceList.state.totalRecords +' records found.');
					that.$('[data-js="hiveSearchResult"]').html(that.hiveResourceList.state.totalRecords  +' records found.');
					that.$('[data-js="hbaseSearchResult"]').html(that.hbaseResourceList.state.totalRecords +' records found.');
					that.$('[data-js="knoxSearchResult"]').html(that.knoxResourceList.state.totalRecords +' records found.');
					that.$('[data-js="stormSearchResult"]').html(that.stormResourceList.state.totalRecords +' records found.');*/
//				}
				XAUtil.blockUI('unblock');
				
			});
		},
		renderTable : function(collName){
			var that = this, tableRegion  = this[collName+'Table'];
			tableRegion.show(new XATableLayout({
				columns: this.getColumns(this[collName]),
				collection: this[collName],
				includeFilter : false,
				scrollToTop : false,
				paginationCache : false,
				gridOpts : {
					row : 	Backgrid.Row.extend({}),
					emptyText : 'No Policies found!'
				}
			}));
		
		},
		getColumns : function(coll){
			var that = this;
			var cols = {
				id : {
					cell : "uri",
					href: function(model){
						var rangerService = new RangerService();
						rangerService.urlRoot += '/name/'+model.get('service'); 
						rangerService.fetch({
						  cache : false,
						  async : false
						});
						return '#!/service/'+rangerService.get('id')+'/policies/'+model.id+'/edit';
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
							if(!_.isUndefined(rawValue))
								return XAUtil.showGroupsOrUsersForPolicy(rawValue, model);
							else 
								return '--';
						}
					}),
					editable : false,
					sortable : false
				},
				//Hack for backgrid plugin doesn't allow to have same column name 
				guid : {
					reName : 'userName',
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					label : localization.tt("lbl.users"),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							if(!_.isUndefined(rawValue))
								return XAUtil.showGroupsOrUsersForPolicy(model.get('policyItems'), model, false);
							else 
								return '--';
						}
					}),
					editable : false,
					sortable : false
				},
			};
			return coll.constructor.getTableCols(cols, coll);
		},
		/** on render callback */
		setupGroupAutoComplete : function(){
			//tags : true,
			/*width :'220px',
			multiple: true,
			minimumInputLength: 1,
			data :this.groupList.map(function(m) {
				//console.log(m);
				return {id : m.id,text : m.get('name')};
			}),*/
			this.groupArr = this.groupList.map(function(m){
				return { id : m.get('name') , text : m.get('name')};
			});
			var that = this, arr = [];
			this.ui.userGroup.select2({
				closeOnSelect : true,
				placeholder : 'Select Group',
				maximumSelectionSize : 1,
				width :'220px',
				tokenSeparators: [",", " "],
				allowClear: true,
				// tags : this.groupArr,
				initSelection : function (element, callback) {
					var data = [];
					console.log(that.groupList);
					
					$(element.val().split(",")).each(function () {
						var obj = _.findWhere(that.groupArr,{id:this});	
						data.push({id: obj.text, text: obj.text});
					});
					callback(data);
				},
				ajax: { 
					url: "service/xusers/groups",
					dataType: 'json',
					data: function (term, page) {
						return {name : term};
					},
					results: function (data, page) { 
						var results = [],selectedVals = [];
						if(!_.isEmpty(that.ui.userGroup.val()))
							selectedVals = that.ui.userGroup.val().split(',');
						if(data.resultSize != "0"){
							results = data.vXGroups.map(function(m, i){	return {id : m.name, text: m.name};	});
							if(!_.isEmpty(selectedVals))
								results = XAUtil.filterResultByIds(results, selectedVals);
							return {results : results};
						}
						return {results : results};
					}
				},	
				formatResult : function(result){
					return result.text;
				},
				formatSelection : function(result){
					return result.text;
				},
				formatNoMatches: function(result){
					return 'No group found.';
				}
			}).on('select2-focus', XAUtil.select2Focus);
		},		
		setupUserAutoComplete : function(){
			var that = this;
			var arr = [];
			this.userArr = this.userList.map(function(m){
				return { id : m.get('name') , text : m.get('name')};
			});
			this.ui.userName.select2({
//				multiple: true,
//				minimumInputLength: 1,
				closeOnSelect : true,
				placeholder : 'Select User',
//				maximumSelectionSize : 1,
				width :'220px',
				tokenSeparators: [",", " "],
				allowClear: true,
				// tags : this.userArr, 
				initSelection : function (element, callback) {
					var data = [];
					$(element.val().split(",")).each(function () {
						var obj = _.findWhere(that.userArr,{id:this});	
						data.push({id: obj.text, text: obj.text});
					});
					callback(data);
				},
				ajax: { 
					url: "service/xusers/users",
					dataType: 'json',
					data: function (term, page) {
						return {name : term};
					},
					results: function (data, page) { 
						var results = [],selectedVals=[];
						if(!_.isEmpty(that.ui.userName.select2('val')))
							selectedVals = that.ui.userName.select2('val');
						if(data.resultSize != "0"){
							results = data.vXUsers.map(function(m, i){	return {id : m.name, text: m.name};	});
							if(!_.isEmpty(selectedVals))
								results = XAUtil.filterResultByIds(results, selectedVals);
							return {results : results};
						}
						return {results : results};
					}
				},	
				formatResult : function(result){
					return result.text;
				},
				formatSelection : function(result){
					return result.text;
				},
				formatNoMatches: function(result){
					return 'No user found.';
				}
				
			}).on('select2-focus', XAUtil.select2Focus);
		},
		/** all post render plugin initialization */
		initializePlugins : function() {
			var that = this;
			this.$(".wrap-header").each(function() {
				var wrap = $(this).next();
				// If next element is a wrap and hasn't .non-collapsible class
				if (wrap.hasClass('wrap') && ! wrap.hasClass('non-collapsible'))
					$(this).append('<a href="#" class="wrap-collapse pull-right">hide&nbsp;&nbsp;<i class="icon-caret-up"></i></a>').append('<a href="#" class="wrap-expand pull-right" style="display: none">show&nbsp;&nbsp;<i class="icon-caret-down"></i></a>');
			});
			// Collapse wrap
			$(document).on("click", "a.wrap-collapse", function() {
				var self = $(this).hide(100, 'linear');
				self.parent('.wrap-header').next('.wrap').slideUp(500, function() {
					$('.wrap-expand', self.parent('.wrap-header')).show(100, 'linear');
				});
				return false;

				// Expand wrap
			}).on("click", "a.wrap-expand", function() {
				var self = $(this).hide(100, 'linear');
				self.parent('.wrap-header').next('.wrap').slideDown(500, function() {
					$('.wrap-collapse', self.parent('.wrap-header')).show(100, 'linear');
				});
				return false;
			});
			
			this.ui.resourceName.bind( "keydown", function( event ) {

				if ( event.keyCode === $.ui.keyCode.ENTER ) {
					that.onSearch();
				}
			});
			
		},
		onSearch : function(e){
			var that = this, type;
			// XAUtil.blockUI();
			//Get search values
			var groups = (this.ui.userGroup.is(':visible')) ? this.ui.userGroup.select2('val'):undefined;
			var users = (this.ui.userName.is(':visible')) ? this.ui.userName.select2('val'):undefined;
			var rxName = this.ui.resourceName.val() || undefined;
			var policyName = this.ui.policyName.val() || undefined;
			var params = {group : groups, user : users, polResource : rxName, policyNamePartial : policyName};
			
			_.each(this.policyCollList, function(obj,i){
				var coll = this[obj.collName];
				//clear previous query params
				_.each(params, function(val, attr){
					delete coll.queryParams[attr];
				});
				//Set default page state
				coll.state = this.defaultPageState;
				coll.queryParams = $.extend(coll.queryParams, params)
            	this.getResourceLists(obj.collName, obj.serviceDefName);
            },this);
			
		},
		autocompleteFilter	: function(e){
			var $el = $(e.currentTarget);
			var $button = $(e.currentTarget).parents('.btn-group').find('button').find('span').first();
			if($el.data('id')== "grpSel"){
				$button.text('Group');
				this.ui.userGroup.show();
				this.setupGroupAutoComplete();
				this.ui.userName.select2('destroy');
				this.ui.userName.val('').hide();
			}else{
				this.ui.userGroup.select2('destroy');
				this.ui.userGroup.val('').hide();
				this.ui.userName.show();
				this.setupUserAutoComplete();
				$button.text('Username');
			}
			//this.onSearch();
		},
		gotoTable : function(e){
			var that = this, elem = $(e.currentTarget),pos;
			var scroll = false;
			if(elem.attr('data-js') == this.ui.gotoHive.attr('data-js')){
				if(that.rHiveTableList.$el.parent('.wrap').is(':visible')){
					pos = that.rHiveTableList.$el.offsetParent().position().top - 100;
					scroll =true;	
				}
			}else if(elem.attr('data-js') == this.ui.gotoHbase.attr('data-js')){
				if(that.rHbaseTableList.$el.parent('.wrap').is(':visible')){
					pos = that.rHbaseTableList.$el.offsetParent().position().top - 100;
					scroll = true;
				}
			}else if(elem.attr('data-js') == this.ui.gotoKnox.attr('data-js')){
				if(that.rKnoxTableList.$el.parent('.wrap').is(':visible')){
					pos = that.rKnoxTableList.$el.offsetParent().position().top - 100;
					scroll = true;
				}
			}else if(elem.attr('data-js') == this.ui.gotoStorm.attr('data-js')){
				if(that.rStormTableList.$el.parent('.wrap').is(':visible')){
					pos = that.rStormTableList.$el.offsetParent().position().top - 100;
					scroll = true;
				}
			}
			if(scroll){
				$("html, body").animate({
					scrollTop : pos
				}, 1100);
			}
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
		},
		/** on close */
		onClose : function() {
		}
	});

	return UserAccessLayout;
});
