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
	var XAGlobals			= require('utils/XAGlobals');
	
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
			btnShowLessUsers 	: '[data-id="showLessUsers"]',
			componentType       : '[data-id="component"]',
			downloadReport      : '[data-id="downloadReport"]'
		},

		/** ui events hash */
		events : function() {
			var events = {};
			events['click ' + this.ui.searchBtn]  = 'onSearch';
			events['click .autoText']  = 'autocompleteFilter';
			events['click .gotoLink']  = 'gotoTable';
			events['click ' + this.ui.btnShowMore]  = 'onShowMore';
			events['click ' + this.ui.btnShowLess]  = 'onShowLess';
			events['click ' + this.ui.btnShowMoreUsers]  = 'onShowMoreUsers';
			events['click ' + this.ui.btnShowLessUsers]  = 'onShowLessUsers';
			events['click ' + this.ui.downloadReport] = 'onDownload';
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
			this.previousSearchUrl = '';
			this.searchedFlag = true;
			this.allowDownload = false;
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
			this.onComponentSelect();
			//Show policies listing for each service and GET policies for each service
			_.each(this.policyCollList, function(obj,i){
				this.renderTable(obj.collName, obj.serviceDefName);
				this.getResourceLists(obj.collName,obj.serviceDefName);
			},this);
			this.modifyTableForSubcolumns();
			this.$el.find('[data-js="policyName"]').focus()
			var url_string = XAUtil.getBaseUrl();
			if(url_string.slice(-1) == "/") {
				url_string = url_string.slice(0,-1);
			}
			this.previousSearchUrl = url_string+"/service/plugins/policies/downloadExcel?";
		},
		
		getResourceLists: function(collName, serviceDefName){

			var that = this, coll = this[collName];
			that.allowDownload = false;
			coll.queryParams.serviceType = serviceDefName;
			coll.fetch({
				cache : false,
				reset: true,
//				async:false,
			}).done(function(){
				coll.trigger('sync')
				XAUtil.blockUI('unblock');
				if(coll.length >= 1 && !that.allowDownload)
					that.allowDownload = true;
				
			});
		},
		renderTable : function(collName,serviceDefName){
			var that = this, tableRegion  = this[collName+'Table'];
			tableRegion.show(new XATableLayout({
				columns: this.getColumns(this[collName],collName,serviceDefName),
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
		getColumns : function(coll,collName,serviceDefName){
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
			permissions: {
				label: 'Permissions',
				cell : Backgrid.HtmlCell.extend({className: 'cellWidth-1', className: 'html-cell' }),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype,{
						fromRaw: function(rawValue,model){
							if(model.get("policyItems").length != 0) {
							    var htmlStr = '';
								var accessStr = '';
								var grpStr = '';
								var userStr = '';
								_.each(model.get("policyItems"),function(policyItem){
									if(_.isEmpty(policyItem.groups)){
										grpStr = '--';
										_.map(policyItem.users,function(el){
											userStr+='<span class="label label-info cellWidth-1 float-left-margin-2" style="">'+el+'</span>';
											});
										accessStr = '';
										_.each(policyItem.accesses,function(obj2){
											accessStr+='<span class="label label-info cellWidth-1 float-left-margin-2" style="">'+obj2.type+'</span>';
									});
									}
									else if(_.isEmpty(policyItem.users)) {
										userStr = '--';
										_.map(policyItem.groups,function(el){
											grpStr+='<span class="label label-info cellWidth-1 float-left-margin-2" style="">'+el+'</span>';
											});
										accessStr = '';
										_.each(policyItem.accesses,function(obj2){
											accessStr+='<span class="label label-info cellWidth-1 float-left-margin-2" style="">'+obj2.type+'</span>';
											});
									}
									else{
										_.map(policyItem.users,function(el){
											userStr+='<span class="label label-info cellWidth-1 float-left-margin-2" style="">'+el+'</span>';
										});
										_.map(policyItem.groups,function(el){
											grpStr+='<span class="label label-info cellWidth-1 float-left-margin-2" style="">'+el+'</span>';
										});
										accessStr = '';
										_.each(policyItem.accesses,function(obj2){
											accessStr+='<span class="label label-info cellWidth-1 float-left-margin-2" style="">'+obj2.type+'</span>';
										});
									}
									htmlStr+='<tr style="height:60px"><td style ="width:80px">'+grpStr+'</td>\
									<td style="width:80px">'+(userStr)+'</td>\
									<td style="width:150px">'+accessStr+'</td></tr>';
									accessStr = '';
									grpStr = '';
									userStr = '';
									});
								return htmlStr;
								}
							else {
								return '---';
							}
						}
					}),
				editable: false,
				sortable: false,
				click: false
				},
				resources:
				{
					label: 'Resources',
					cell: 'Html',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue,model) {
							var strVal = '';
							var res = model.get('resources');
							_.each(res,function(res_obj,key){
								strVal += "<b>"+key+":</b>";
								_.map(res_obj.values,function(ob){
									strVal +="<span title='"+ob+"'>"+ ob+"</span>" + ",";
								});
								strVal = strVal.slice(0,-1);
								strVal = strVal+ "<br />";
							});
							return strVal;
							}
					}),
			editable: false,
			sortable: false,
			click: false
					}
			};

			return coll.constructor.getTableCols(cols, coll);
		},
		/* components */
	onComponentSelect: function(){
		var that = this;
		var options = this.serviceDefList.map(function(m){ return { 'id' : m.get('name'), 'text' : m.get('name')}; });
		this.ui.componentType.select2({
			multiple: true,
			closeOnSelect: true,
			placeholder: 'Select Component',
		    //maximumSelectionSize : 1,
		    width: '220px',
		    allowClear: true,
		    data: options
		});
	},
	modifyTableForSubcolumns : function(){
		this.$el.find(".permissions").html('<tr><th colspan="3">Permissions</th></tr>\
							<tr><th style="width:80px">Groups</th><th style="width:80px">Users</th>\
							<th style="width:150px">Accesses</th></tr>');
	},
	onDownload: function(e){
		if(!this.allowDownload){
			XAUtil.alertPopup({
				msg :"No policies found to download!",
			});
			return;
		}
		var rangerPolicyList = new RangerPolicyList();
		var that = this;
		var url = '';
		var groups = (this.ui.userGroup.is(':visible')) ? this.ui.userGroup.select2('val'):undefined;
		if(groups == "") {
			groups = undefined;
		}
		var users = (this.ui.userName.is(':visible')) ? this.ui.userName.select2('val'):undefined;
		var rxName = this.ui.resourceName.val() || undefined;
		var policyName = this.ui.policyName.val() || undefined;
		var params = {group : groups, user : users, polResource : rxName, policyNamePartial : policyName};
		var component = (this.ui.componentType.val() != "") ? this.ui.componentType.select2('val'):undefined;
		_.each(that.policyCollList, function(obj,i){
			var coll = that[obj.collName];
		},this);
		var btn = $(e.currentTarget);
		if(!this.searchedFlag) {
			var url_string = XAUtil.getBaseUrl();
			if(url_string.slice(-1) == "/") {
				url_string = url_string.slice(0,-1);
			}
			url = url_string + that.getDownloadExcelUrl(this,component,params);
			this.previousSearchUrl = url;
		 }
		else {
			url =  this.previousSearchUrl;
		}
		$('<a href="'+url+'" download hidden>').appendTo('body').get(0).click();

	},
	getDownloadExcelUrl: function(that,component,params){
		var queryParams = {};
		var data = {};
		var comp_str = '';
		if(!_.isUndefined(component)) {
			_.each(component,function(comp){
				comp_str = comp_str + comp + '_';
				});
		}
		if (comp_str != '') {
			comp_str = comp_str.slice(0,-1);
		}
		queryParams = that['hdfsPolicyList'].queryParams;
		_.map(params, function(value, key) {
			if (!_.isUndefined(value)) {
				data[key] = value;
			}
		});
		if(!_.isUndefined(queryParams.group)) {
			data.group = queryParams.group;
		}
		if(!_.isUndefined(queryParams.user)) {
			data.user = queryParams.user;
		}
		if(!_.isUndefined(queryParams.polResource)) {
			data.polResource = queryParams.polResource;
		}
		if(!_.isUndefined(queryParams.policyNamePartial)) {
			data.policyNamePartial = queryParams.policyNamePartial;
		}
		var policyList;
		var url = '/service/plugins/policies/downloadExcel?';
		var str = jQuery.param( data );
		url = url + str + "&serviceType="+comp_str;
		return url;
	},
		/** on render callback */
		setupGroupAutoComplete : function(){
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
			})//.on('select2-focus', XAUtil.select2Focus);
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
				
			})//.on('select2-focus', XAUtil.select2Focus);
		},
		/** all post render plugin initialization */
		initializePlugins : function() {
			var that = this;
			this.$(".wrap-header").each(function() {
				var wrap = $(this).next();
				// If next element is a wrap and hasn't .non-collapsible class
				if (wrap.hasClass('wrap') && ! wrap.hasClass('non-collapsible'))
					$(this).prepend('<a href="#" class="wrap-collapse btn-right">hide&nbsp;&nbsp;<i class="icon-caret-up"></i></a>').prepend('<a href="#" class="wrap-expand btn-right" style="display: none">show&nbsp;&nbsp;<i class="icon-caret-down"></i></a>');
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
			//Get search values
			var groups = (this.ui.userGroup.is(':visible')) ? this.ui.userGroup.select2('val'):undefined;
			var users = (this.ui.userName.is(':visible')) ? this.ui.userName.select2('val'):undefined;
			var rxName = this.ui.resourceName.val() || undefined;
			var policyName = this.ui.policyName.val() || undefined;
			var params = {group : groups, user : users, polResource : rxName, policyNamePartial : policyName};
			var component = (this.ui.componentType.val() != "") ? this.ui.componentType.select2('val'):undefined;
			var compFlag = false;
			var showTabs = false;
			if(_.isUndefined(users) && _.isUndefined(rxName) && _.isUndefined(policyName) && (groups==""))	{
				showTabs = false;
			}
			else {
				showTabs = true;
			}
			if(!_.isUndefined(component)) {
				compFlag = true;
			}
			else {
				compFlag = false;
			}
			that.$el.find('[data-compHeader]').show();
			that.$el.find('[data-comp]').show();
			if(compFlag) { //if components selected
				that.$el.find('[data-compHeader]').hide();
				that.$el.find('[data-comp]').hide();
				_.each(that.policyCollList, function(obj,i){
					_.each(component,function(comp){
						if(comp === obj.serviceDefName) {
							var coll = that[obj.collName];
							//clear previous query params
							_.each(params, function(val, attr){
								delete coll.queryParams[attr];
							});
							//Set default page state
							coll.state = that.defaultPageState;
							coll.queryParams = $.extend(coll.queryParams, params);
							that.getResourceLists(obj.collName, obj.serviceDefName);
							that.$el.find('[data-compHeader="'+comp+'"]').show();
							that.$el.find('[data-comp="'+comp+'"]').show();
							}
						});
					},this);
				}
			else { // show all tables if no search component values selected
				that.$el.find('[data-compHeader]').show();
				that.$el.find('[data-comp]').show();
				_.each(this.policyCollList, function(obj,i){
					var coll = this[obj.collName];
					//clear previous query params
					_.each(params, function(val, attr){
						delete coll.queryParams[attr];
					});
					//Set default page state
					coll.state = this.defaultPageState;
					coll.queryParams = $.extend(coll.queryParams, params);
					this.getResourceLists(obj.collName, obj.serviceDefName);
					},this);
				}
					var rangerPolicyList = new RangerPolicyList();
					var url = '';
					if (groups == "") {
						groups = undefined;
					}
					params = {
						group : groups,
						user : users,
						polResource : rxName,
						policyNamePartial : policyName
					};
					var url_string = XAUtil.getBaseUrl();
					if(url_string.slice(-1) == "/") {
						url_string = url_string.slice(0,-1);
					}
					url = url_string
							+ this.getDownloadExcelUrl(this, component,
									params);
					this.previousSearchUrl = url;
					this.searchedFlag = true;
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
