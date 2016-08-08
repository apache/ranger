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
	var CustomSubgrid 		= require('views/common/CustomSubgrid');
	
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
			downloadReport      : '[data-id="downloadReport"]',
			policyType          : '[data-id="policyType"]',
			btnShowMoreAccess 	: '[data-id="showMoreAccess"]',
			btnShowLessAccess 	: '[data-id="showLessAccess"]'
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
			events['click .downloadFormat'] = 'setDownloadFormatFilter';
			events['click ' + this.ui.btnShowMoreAccess] = 'onShowMorePermissions';
			events['click ' + this.ui.btnShowLessAccess] = 'onShowLessPermissions';
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
			this.searchedFlag = false;
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
			this.renderComponentAndPolicyTypeSelect();
			//Show policies listing for each service and GET policies for each service
			_.each(this.policyCollList, function(obj,i){
				this.renderTable(obj.collName, obj.serviceDefName);
				this.getResourceLists(obj.collName,obj.serviceDefName);
			},this);
			this.$el.find('[data-js="policyName"]').focus()
			var urlString = XAUtil.getBaseUrl();
			if(urlString.slice(-1) == "/") {
				urlString = urlString.slice(0,-1);
			}
			this.previousSearchUrl = urlString+"/service/plugins/policies/downloadExcel?";
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
				_.each(that[collName].models,function(model,ind){
					if (XAUtil.isMaskingPolicy(model.get('policyType'))) {
						//'<name>Collection' must be same as subgrid custom column name
						model.attributes.allowCollection = model.get('dataMaskPolicyItems');
					} else if (XAUtil.isRowFilterPolicy(model.get('policyType'))) {
						model.attributes.allowCollection = model.get('rowFilterPolicyItems');
					} else {
						model.attributes.allowCollection = model.get('policyItems');
					}
					model.attributes.denyCollection  = model.get('denyPolicyItems');
					model.attributes.denyExcludeCollection    = model.get('denyExceptions');
					model.attributes.allowExcludeCollection = model.get('allowExceptions');
				});

			});
		},
		renderTable : function(collName,serviceDefName){
			var that = this, tableRegion  = this[collName+'Table'];
			tableRegion.show(new XATableLayout({
				columns: this.getSubgridColumns(this[collName],collName,serviceDefName),
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
		getSubgridColumns:function(coll,collName,serviceDefName){
			var that = this;

			var subcolumns = [{
					name: 'groups',
					cell: 'html',
					label: 'Groups',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue,model, coll) {
							var startSpanEle = '<span class="label label-info cellWidth-1 float-left-margin-2" style="">',endSpanEle = '</span>';
							var group_str = '';
							if(_.isEmpty(model.get('groups'))){
								return '<center>--</center>';
							} else {
								_.each(model.get('groups'),function(group,index){
									if(index < 4) {
										group_str += '<span class="label label-info cellWidth-1 float-left-margin-2" group-policy-id="'+model.cid+'" style="">' + _.escape(group) + endSpanEle  + " ";
									} else {
										group_str += '<span class="label label-info cellWidth-1 float-left-margin-2" group-policy-id="'+model.cid+'" style="display:none">' + _.escape(group) + endSpanEle  + " ";
									}
								});
								if(model.get('groups').length > 4) {
									group_str += '<span class="pull-left float-left-margin-2">\
									<a href="javascript:void(0);" data-id="showMoreAccess" policy-id="'+
									model.cid+'"><code style=""> + More..</code></a></span>\
									<span class="pull-left float-left-margin-2" ><a href="javascript:void(0);" data-id="showLessAccess" policy-id="'+
									model.cid+'" style="display:none;"><code style=""> - Less..</code></a></span>';}
									return group_str;
								}
						}
					}),
					editable: false,
					click: false,
					sortable: false
				},
				{
					name: 'users',
					cell: 'html',
					label: 'Users',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue,model) {
							var startSpanEle = '<span class="label label-info cellWidth-1 float-left-margin-2" style="">',endSpanEle = '</span>';
							var user_str = '';
							if(_.isEmpty(model.get('users'))){
								return '<center>--</center>';
							} else {
								_.each(model.get('users'),function(user,index){
									if(index < 4) {
										user_str += '<span class="label label-info cellWidth-1 float-left-margin-2" user-policy-id="'+model.cid+'" style="">' + _.escape(user) + endSpanEle  + " ";
									} else {
										user_str += '<span class="label label-info cellWidth-1 float-left-margin-2" user-policy-id="'+model.cid+'" style="display:none">' + _.escape(user) + endSpanEle  + " ";
									}
								});
								if(model.get('users').length > 4) {
									user_str += '<span class="pull-left float-left-margin-2">\
									<a href="javascript:void(0);" data-id="showMoreAccess" policy-id="'+
									model.cid+'"><code style=""> + More..</code></a></span>\
									<span class="pull-left float-left-margin-2" ><a href="javascript:void(0);" data-id="showLessAccess" policy-id="'+
									model.cid+'" style="display:none;"><code style=""> - Less..</code></a></span>';}
									return user_str;}
								}
					}),
					editable: false,
					click: false,
					sortable: false
				},
				{ 
					name: 'accesses',
					cell: 'html',
					label: 'Accesses',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue,model) {
							var startSpanEle = '<span class="label label-info cellWidth-1 float-left-margin-2" style="">',endSpanEle = '</span>';
							var access_str = '';
							_.each(model.get('accesses'),function(access,index){
								if(index < 4){
									access_str += '<span class="label label-info cellWidth-1 float-left-margin-2" access-policy-id="'+model.cid+'" style="">' + access.type+ endSpanEle  + " ";
								} else {
									access_str += '<span class="label label-info cellWidth-1 float-left-margin-2" access-policy-id="'+model.cid+'" style="display:none">' + access.type+ endSpanEle  + " ";
								}
							});
							if(model.get('accesses').length > 4) {
								access_str += '<span class="pull-left float-left-margin-2">\
								<a href="javascript:void(0);" data-id="showMoreAccess" policy-id="'+
								model.cid+'"><code style=""> + More..</code></a></span>\
								<span class="pull-left float-left-margin-2" ><a href="javascript:void(0);" data-id="showLessAccess" policy-id="'+
				                model.cid+'" style="display:none;"><code style=""> - Less..</code></a></span>';}
								return access_str;
						}
					}),
					editable: false,
					click: false,
					sortable: false
				}
			];
			var columns = {
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
				resources: {
					label: 'Resources',
					cell: 'Html',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue,model) {
							var strVal = '', names = '';
							var resource = model.get('resources');
							_.each(resource,function(resourceObj,key){
								strVal += "<b>"+key+":</b>";
								strVal += "<span title='";
								names = '';
								_.map(resourceObj.values,function(resourceVal){
									names += _.escape(resourceVal)+",";
								});
								names = names.slice(0,-1);
								strVal += names + "'>"+names +"</span>";
								strVal = strVal+ "<br />";
							});
							return strVal;
						}
					}),
					editable: false,
					sortable: false,
					click: false
					},
				policyType: {
					label: 'Policy Type',
					cell: Backgrid.HtmlCell.extend({className: 'cellWidth-1', className: 'html-cell'}),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype,{
						fromRaw: function(rawValue,model){
							var policyType = model.get("policyType");
							var startLbl = '<label class="label label-ranger" style="float:inherit;">';
							if (XAUtil.isMaskingPolicy(policyType)) {
								return startLbl + XAEnums.RangerPolicyType.RANGER_MASKING_POLICY_TYPE.label + '</label>';
							} else if (XAUtil.isRowFilterPolicy(policyType)) {
								return startLbl + XAEnums.RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.label + '</label>';
							}else{// by default it is access
								return startLbl + XAEnums.RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.label + '</label>';
							}
						}
					}),
					editable: false,
					sortable: false,
					click: false
				},
				isEnabled:{
					label:localization.tt('lbl.status'),
					cell :"html",
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							return rawValue ? '<label class="label label-success" style="float:inherit;">Enabled</label>' : '<label class="label label-important" style="float:inherit;">Disabled</label>';
						}
					}),
					click : false,
					drag : false,
					sortable : false
				}
			};
			var permissions = this.getPermissionColumns(this[collName],collName,serviceDefName,subcolumns);
			_.extend(columns,permissions);
			return coll.constructor.getTableCols(columns, coll);
		},
		getPermissionColumns: function (coll,collName,serviceDefName,subcolumns){

			var that = this,permissions,cols,serviceDefOptions;

			var serviceDefOptions = {};
			_.each(this.serviceDefList.models, function(model){
				if(model.get('name') === serviceDefName)
					serviceDefOptions = model.get('options');
			});
			var enableDenyAndExceptionsInPolicies = false;
			if(!_.isUndefined(serviceDefOptions.enableDenyAndExceptionsInPolicies))
				 enableDenyAndExceptionsInPolicies = true;
			if(serviceDefName === XAEnums.ServiceType.SERVICE_TAG.label){
				enableDenyAndExceptionsInPolicies = true;
			}

            permissions = {
        		allow:{
					label: 'Allow Conditions',
					cell: "subgrid-custom",
					optionValues : subcolumns,
					editable: false,
					sortable: false,
					click : false
		        }
            };
            if(enableDenyAndExceptionsInPolicies) {
            	cols = {
            			allowExclude:{
            				label: 'Allow Exclude',
            				cell: "subgrid-custom",
            				optionValues : subcolumns,
            				editable: false,
            				sortable: false,
            				click : false
            			},	
            			deny:{
            				label: 'Deny Conditions',
            				cell: "subgrid-custom",
            				optionValues : subcolumns,
            				editable: false,
            				sortable: false,
            				click : false
            			},
            			denyExclude:{
            				label: 'Deny Exclude',
            				cell: "subgrid-custom",
            				optionValues : subcolumns,
            				editable: false,	
            				sortable: false,
            				click : false
            			}
            	};
            }
			return _.extend(permissions,cols);
		},

		/* add 'component' and 'policy type' select */
		renderComponentAndPolicyTypeSelect: function(){
			var that = this;
			var options = this.serviceDefList.map(function(m){ return { 'id' : m.get('name'), 'text' : m.get('name')}; });
			var policyTypes = _.map(XAEnums.RangerPolicyType,function(m){
				return {'id': m.value,'text': m.label};
			});
			this.ui.componentType.select2({
				multiple: true,
				closeOnSelect: true,
				placeholder: 'Select Component',
			    //maximumSelectionSize : 1,
			    width: '220px',
			    allowClear: true,
			    data: options
			});
			this.ui.policyType.select2({
				closeOnSelect: true,
				placeholder: 'Select policy type',
				maximumSelectionSize : 1,
				width: '220px',
				allowClear: true,
				data: policyTypes
	
			});
		},
		onDownload: function(e){
			var that = this, url = '';
			if(!this.allowDownload){
				XAUtil.alertPopup({
					msg :"No policies found to download!",
				});
				return;
			}
			if(!this.searchedFlag) {
				url =  this.previousSearchUrl;
			} else if (this.searchedFlag && this.updatedUrl) {
				var urlString = XAUtil.getBaseUrl();
				if(urlString.slice(-1) === "/") {
					urlString = urlString.slice(0,-1);
				}
				url = url + urlString;
				if (e === "xlsFormat") {
						url = url + '/service/plugins/policies/downloadExcel?';
				} else {
						url = url + '/service/plugins/policies/csv?';
				}
				url = url + this.searchedParamsString + this.searchedComponentString;
				this.previousSearchUrl = url;
				this.searchedFlag = true;
			}
			this.ui.downloadReport.attr("href",url)[0].click();
		},
		setDownloadReportUrl: function(that,component,params){

			var compString = '', url = '';
			if(!_.isUndefined(component)) {
				_.each(component,function(comp){
					compString = compString + comp + '_';
					});
			}
			if (!_.isEmpty(compString)) {
				compString = compString.slice(0,-1);
			}
			_.each(params, function(val, paramName){
				if(_.isUndefined(val) || _.isEmpty(val)) {
					delete params[paramName];
				}
			});
			var str = jQuery.param( params );
			this.searchedComponentString = "&serviceType=" + compString;
			this.searchedParamsString = str;
			this.updatedUrl = true;

		},	
		/** on render callback */
		setupGroupAutoComplete : function(){
			this.groupArr = this.groupList.map(function(m){
				return { id : m.get('name') , text : _.escape(m.get('name'))};
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
							results = data.vXGroups.map(function(m, i){	return {id : m.name, text: _.escape(m.name) };	});
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
				return { id : m.get('name') , text : _.escape(m.get('name')) };
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
							results = data.vXUsers.map(function(m, i){	return {id : m.name, text: _.escape(m.name) };	});
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
			var that = this, url = '', urlString = XAUtil.getBaseUrl();
			//Get search values
			var groups = (this.ui.userGroup.is(':visible')) ? this.ui.userGroup.select2('val'):undefined;
			var users = (this.ui.userName.is(':visible')) ? this.ui.userName.select2('val'):undefined;
			var rxName = this.ui.resourceName.val(), policyName = this.ui.policyName.val() , policyType = this.ui.policyType.val();
			var params = {group : groups, user : users, polResource : rxName, policyNamePartial : policyName, policyType: policyType};
			var component = (this.ui.componentType.val() != "") ? this.ui.componentType.select2('val'):undefined;

			var compFlag = false;
			if(!_.isUndefined(component)) {
				compFlag = true;
			} else {
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
			} else {
				// show all tables if no search component values selected
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
			params = {
				group : groups,
				user : users,
				polResource : rxName,
				policyNamePartial : policyName,
				policyType: policyType
			};

			this.setDownloadReportUrl(this,component,params);
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
			} else {
				this.ui.userGroup.select2('destroy');
				this.ui.userGroup.val('').hide();
				this.ui.userName.show();
				this.setupUserAutoComplete();
				$button.text('Username');
			}
		},
		setDownloadFormatFilter : function(e){
			var that = this;
			var el = $(e.currentTarget);
			if(el.data('id') === "xlsFormat") {
				if(!that.searchedFlag) {
					var urlString = XAUtil.getBaseUrl();
					if(urlString.slice(-1) === "/") {
						urlString = urlString.slice(0,-1);
					}
				}
				this.previousSearchUrl = urlString + "/service/plugins/policies/downloadExcel?";
			} else {
				if(!that.searchedFlag) {
					var urlString = XAUtil.getBaseUrl();
					if(urlString.slice(-1) === "/") {
						urlString = urlString.slice(0,-1);
					}
					this.previousSearchUrl = urlString + "/service/plugins/policies/csv?";
				}
			}
			this.onDownload(el.data('id'));
		},
		gotoTable : function(e){
			var that = this, elem = $(e.currentTarget),pos;
			var scroll = false;
			if(elem.attr('data-js') == this.ui.gotoHive.attr('data-js')){
				if(that.rHiveTableList.$el.parent('.wrap').is(':visible')){
					pos = that.rHiveTableList.$el.offsetParent().position().top - 100;
					scroll =true;	
				}
			} else if(elem.attr('data-js') == this.ui.gotoHbase.attr('data-js')){
				if(that.rHbaseTableList.$el.parent('.wrap').is(':visible')){
					pos = that.rHbaseTableList.$el.offsetParent().position().top - 100;
					scroll = true;
				}
			} else if(elem.attr('data-js') == this.ui.gotoKnox.attr('data-js')){
				if(that.rKnoxTableList.$el.parent('.wrap').is(':visible')){
					pos = that.rKnoxTableList.$el.offsetParent().position().top - 100;
					scroll = true;
				}
			} else if(elem.attr('data-js') == this.ui.gotoStorm.attr('data-js')){
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
		onShowMorePermissions: function(e){
						var policyId = $(e.currentTarget).attr('policy-id');
						var $td = $(e.currentTarget).parents('td');
						$td.find('[access-policy-id="'+policyId+'"]').show();
						$td.find('[user-policy-id="'+policyId+'"]').show();
						$td.find('[group-policy-id="'+policyId+'"]').show();
						$td.find('[data-id="showLessAccess"][policy-id="'+policyId+'"]').show();
						$td.find('[data-id="showMoreAccess"][policy-id="'+policyId+'"]').hide();

					},
					onShowLessPermissions: function(e){
						var policyId = $(e.currentTarget).attr('policy-id');
						var $td = $(e.currentTarget).parents('td');
						$td.find('[access-policy-id="'+policyId+'"]').slice(4).hide();
						$td.find('[user-policy-id="'+policyId+'"]').slice(4).hide();
						$td.find('[group-policy-id="'+policyId+'"]').slice(4).hide();
						$td.find('[data-id="showMoreAccess"][policy-id="'+policyId+'"]').show();
						$td.find('[data-id="showLessAccess"][policy-id="'+policyId+'"]').hide();

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
