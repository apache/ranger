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

	var Backbone 			= require('backbone');
	var XALinks 			= require('modules/XALinks');
	var XAEnums 			= require('utils/XAEnums');
	var XAUtil				= require('utils/XAUtils');
	var XABackgrid			= require('views/common/XABackgrid');
	var XATableLayout		= require('views/common/XATableLayout');
	var localization		= require('utils/XALangSupport');
	
	var VXResourceList 		= require('collections/VXResourceList');
	var UseraccesslayoutTmpl= require('hbs!tmpl/reports/UserAccessLayout_tmpl');

	var UserAccessLayout 	= Backbone.Marionette.Layout.extend(
	/** @lends UserAccessLayout */
	{
		_viewName : 'UserAccessLayout',

		template : UseraccesslayoutTmpl,
		breadCrumbs : [XALinks.get('UserAccessReport')],
		templateHelpers :function(){
			return {groupList : this.groupList};
		},

		/** Layout sub regions */
		regions : {
			'rHdfsTableList'	: 'div[data-id="r_hdfsTable"]',
			'rHiveTableList'	: 'div[data-id="r_hiveTable"]',
			'rHbaseTableList'	: 'div[data-id="r_hbaseTable"]',
			'rKnoxTableList'	: 'div[data-id="r_knoxTable"]',
			'rStormTableList'	: 'div[data-id="r_stormTable"]',
			'rHdfsTableSpinner' : '[data-id="r_hdfsTableSpinner"]',
			'rHiveTableSpinner' : '[data-id="r_hiveTableSpinner"]',
			'rHbaseTableSpinner': '[data-id="r_hbaseTableSpinner"]',
			'rKnoxTableSpinner' : '[data-id="r_knoxTableSpinner"]',
			'rStormTableSpinner': '[data-id="r_stormTableSpinner"]'
			
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
			
			var params = [];
			this.hdfsResourceList = new VXResourceList();
			this.hiveResourceList = new VXResourceList();
			this.hbaseResourceList = new VXResourceList();
			this.knoxResourceList = new VXResourceList();
			this.stormResourceList = new VXResourceList();
			this.bindEvents();
			
		},		   

		/** all events binding here */
		bindEvents : function() {
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
			this.listenTo(this.hiveResourceList, "change:foo", function(){alert();}, this);
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
				return { id : m.id , text : m.get('name')};
			});
			var that = this, arr = [];
			this.ui.userGroup.select2({
				closeOnSelect : true,
				placeholder : 'Select Group',
			//	maximumSelectionSize : 1,
				width :'220px',
				tokenSeparators: [",", " "],
				tags : this.groupArr,
				initSelection : function (element, callback) {
					var data = [];
					console.log(that.groupList);
					
					$(element.val().split(",")).each(function () {
						var obj = _.findWhere(that.groupArr,{id:this});	
						data.push({id: this, text: obj.text});
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
								results = data.vXGroups.map(function(m, i){	return {id : (m.id).toString(), text: m.name};	});
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
				return { id : m.id , text : m.get('name')};
			});
			this.ui.userName.select2({
//				multiple: true,
//				minimumInputLength: 1,
				closeOnSelect : true,
				placeholder : 'Select User',
		//		maximumSelectionSize : 1,
				width :'220px',
				tokenSeparators: [",", " "],
				tags : this.userArr, 
				initSelection : function (element, callback) {
					var data = [];
					$(element.val().split(",")).each(function () {
						var obj = _.findWhere(that.userArr,{id:this});	
						data.push({id: this, text: obj.text});
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
								results = data.vXUsers.map(function(m, i){	return {id : m.id+"", text: m.name};	});
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
		getResourceLists: function(params){
			var that = this;
			
			var resourceList = new VXResourceList();
			if(!_.isUndefined(params)){
				XAUtil.blockUI();
				resourceList.setPageSize(200, {fetch : false});
				resourceList.fetch({
					data : params,
					cache : false,
					success : function(){
						XAUtil.blockUI('unblock');
					},
					error : function(){XAUtil.blockUI('unblock');}
				}).done(function(){
					//console.log(resourceList);
					XAUtil.blockUI('unblock');
				//console.log(resourceList);
				XAUtil.blockUI('unblock');
				var obj = resourceList.groupBy('assetType');
					var hdfsList = !_.isUndefined(obj[XAEnums.AssetType.ASSET_HDFS.value]) ? obj[XAEnums.AssetType.ASSET_HDFS.value] : [];
					that.hdfsResourceList.reset(hdfsList);
					var hiveList = !_.isUndefined(obj[XAEnums.AssetType.ASSET_HIVE.value]) ? obj[XAEnums.AssetType.ASSET_HIVE.value] :  [];
					that.hiveResourceList.reset(hiveList);
					var hbaseList= !_.isUndefined(obj[XAEnums.AssetType.ASSET_HBASE.value]) ? obj[XAEnums.AssetType.ASSET_HBASE.value] : [];
					that.hbaseResourceList.reset(hbaseList);
					var knoxList= !_.isUndefined(obj[XAEnums.AssetType.ASSET_KNOX.value]) ? obj[XAEnums.AssetType.ASSET_KNOX.value] : [];
					that.knoxResourceList.reset(knoxList);
					var stormList= !_.isUndefined(obj[XAEnums.AssetType.ASSET_STORM.value]) ? obj[XAEnums.AssetType.ASSET_STORM.value] : [];
					that.stormResourceList.reset(stormList);
					
					
					if(!_.isEmpty(params)){
						var totalRecords = hdfsList.length + hiveList.length + hbaseList.length + knoxList.length + stormList.length;
						that.$('[data-js="searchResult"]').html('Total '+totalRecords+' records found.');
						that.$('[data-js="hdfsSearchResult"]').html(hdfsList.length +' records found.');
						that.$('[data-js="hiveSearchResult"]').html(hiveList.length  +' records found.');
						that.$('[data-js="hbaseSearchResult"]').html(hbaseList.length +' records found.');
						that.$('[data-js="knoxSearchResult"]').html(knoxList.length +' records found.');
						that.$('[data-js="stormSearchResult"]').html(stormList.length +' records found.');
					}
				});			
			}else{
				var obj = this.collection.groupBy('assetType');
				var hdfsList = !_.isUndefined(obj[XAEnums.AssetType.ASSET_HDFS.value]) ? obj[XAEnums.AssetType.ASSET_HDFS.value] : [];
				that.hdfsResourceList.reset(hdfsList);
				var hiveList = !_.isUndefined(obj[XAEnums.AssetType.ASSET_HIVE.value]) ? obj[XAEnums.AssetType.ASSET_HIVE.value] :  [];
				that.hiveResourceList.reset(hiveList);
				var hbaseList= !_.isUndefined(obj[XAEnums.AssetType.ASSET_HBASE.value]) ? obj[XAEnums.AssetType.ASSET_HBASE.value] : [];
				that.hbaseResourceList.reset(hbaseList);
				var knoxList= !_.isUndefined(obj[XAEnums.AssetType.ASSET_KNOX.value]) ? obj[XAEnums.AssetType.ASSET_KNOX.value] : [];
				that.knoxResourceList.reset(knoxList);
				var stormList= !_.isUndefined(obj[XAEnums.AssetType.ASSET_STORM.value]) ? obj[XAEnums.AssetType.ASSET_STORM.value] : [];
				that.stormResourceList.reset(stormList);
			}
				
		},
		onRender : function() {
			this.initializePlugins();
			this.setupGroupAutoComplete();
			this.getResourceLists();
			this.renderHdfsTable();
			this.renderHiveTable();
			this.renderHbaseTable();
			this.renderKnoxTable();
			this.renderStormTable();
			//this.listenTo(this.hiveResourceList, "reset", function(){alert();}, this);
		},
		renderHdfsTable : function(){
			var that = this;

			this.rHdfsTableList.show(new XATableLayout({
				columns: this.getHdfsColumns(),
				collection: this.hdfsResourceList,
				includeFilter : false,
				includePagination : false,
				gridOpts : {
					row : 	Backgrid.Row.extend({}),
					header : XABackgrid,
					emptyText : 'No Policies found!'
				}
				/*filterOpts : {
				  name: ['name'],
				  placeholder: localization.tt('plcHldr.searchByResourcePath'),
				  wait: 150
				}*/
			}));
		
		},
		renderHiveTable : function(){
			var that = this;

			this.rHiveTableList.show(new XATableLayout({
				columns: this.getHiveColumns(),
				collection: this.hiveResourceList,
				includeFilter : false,
				includePagination : false,
				gridOpts : {
					//row: TableRow,
					header : XABackgrid,
					emptyText : 'No Policies found!'
				}
				/*filterOpts : {
				  name: ['name'],
				  placeholder: localization.tt('plcHldr.searchByResourcePath'),
				  wait: 150
				}*/
			}));
		
		},
		renderHbaseTable : function(){
			var that = this;

			this.rHbaseTableList.show(new XATableLayout({
				columns: this.getHbaseColumns(),
				collection: this.hbaseResourceList,
				includeFilter : false,
				includePagination : false,
				gridOpts : {
					//row: TableRow,
					header : XABackgrid,
					emptyText : 'No Policies found!'
				}
				/*filterOpts : {
				  name: ['name'],
				  placeholder: localization.tt('plcHldr.searchByResourcePath'),
				  wait: 150
				}*/
			}));
		
		},
		renderKnoxTable : function(){
			var that = this;

			this.rKnoxTableList.show(new XATableLayout({
				columns: this.getKnoxColumns(),
				collection: this.knoxResourceList,
				includeFilter : false,
				includePagination : false,
				gridOpts : {
					//row: TableRow,
					header : XABackgrid,
					emptyText : 'No Policies found!'
				}
				/*filterOpts : {
				  name: ['name'],
				  placeholder: localization.tt('plcHldr.searchByResourcePath'),
				  wait: 150
				}*/
			}));
		
		},
		renderStormTable : function(){
			var that = this;

			this.rStormTableList.show(new XATableLayout({
				columns: this.getStormColumns(),
				collection: this.stormResourceList,
				includeFilter : false,
				includePagination : false,
				gridOpts : {
					//row: TableRow,
					header : XABackgrid,
					emptyText : 'No Policies found!'
				}
				/*filterOpts : {
				  name: ['name'],
				  placeholder: localization.tt('plcHldr.searchByResourcePath'),
				  wait: 150
				}*/
			}));
		
		},
		getHdfsColumns : function(){
			var that = this;
			var cols = {
				policyName : {
					cell : "uri",
					href: function(model){
						return '#!/hdfs/'+model.get('assetId')+'/policy/' + model.id;
					},
					label	: localization.tt("lbl.policyName"),
					editable: false,
					sortable : false
				},	
				name : {
					cell : "html",
					label	: localization.tt("lbl.resourcePath"),
					editable: false,
					sortable : false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							return _.isUndefined(rawValue) ? '--': '<span title="'+rawValue+'">'+rawValue+'</span>';  
						}
					})
				},
				assetName : {
					label : localization.tt("lbl.repository"),
					cell: "String",
					click : false,
					drag : false,
					editable:false,
					sortable: false
				},
				isRecursive:{
					label:localization.tt('lbl.includesAllPathsRecursively'),
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
							//return status ? "Yes" : "No";
							return status  ? '<label class="label label-success">YES</label>' : '<label class="label label">NO</label>';
						}
					}),
					click : false,
					drag : false,
					sortable: false
				},
				auditList : {
					label : localization.tt("lbl.auditLogging"),
					cell: "html",
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							//return model.has('auditList') ? 'On' : 'Off';
							return model.has('auditList') ? '<label class="label label-success">ON</label>' : '<label class="label label">OFF</label>';
						}
					}),
					click : false,
					drag : false,
					sortable: false,
					editable:false
				},
				permMapListUser : {
					name : 'permMapList',
					reName : 'userName',
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					label : localization.tt("lbl.users"),
				//	canHeaderSearch : true,
				//	headerSearchStyle  : 'width:85%;',
					placeholder : 'User(s)',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							var showMoreLess = false;
							if(_.isArray(rawValue))
								rawValue =  new Backbone.Collection(rawValue);
							if(!_.isUndefined(rawValue) && rawValue.models.length > 0){
								var userArr = _.uniq(_.compact(_.map(rawValue.models, function(m, i){
									if(m.has('userName'))
										return m.get('userName') ;
								})));
								if(userArr.length > 0)
									var resourceId =rawValue.models[0].attributes.resourceId; 
								var newUserArr = _.map(userArr, function(name, i){
									if(i >=  4)
										return '<span class="label label-info" policy-user-id="'+resourceId+'" style="display:none;">' + name + '</span>';
									else if(i == 3 && userArr.length > 4){
										showMoreLess = true;
										return '<span class="label label-info" policy-user-id="'+resourceId+'">' + name + '</span>';
									}
									else
										return '<span class="label label-info" policy-user-id="'+resourceId+'">' + name + '</span>';
								});
								if(showMoreLess){
									newUserArr.push('<span class="pull-left"><a href="javascript:void(0);" data-id="showMoreUsers" class="" policy-user-id="'+resourceId+'"><code> + More..</code></a></span>\
											<span class="pull-left" ><a href="javascript:void(0);" data-id="showLessUsers" class="" policy-user-id="'+resourceId+'" style="display:none;"><code> - Less..</code></a></span>');
								}
								return newUserArr.length ? newUserArr.join(' ') : '--';
							}else
								return '--';
						}
					}),
					editable:false,
					sortable: false
				},
				permMapList : {
					reName : 'groupName',
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					label : localization.tt("lbl.groups"),
				//	canHeaderSearch : true,
				//	headerSearchStyle  : 'width:85%;',
					placeholder : 'Group Name',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							return XAUtil.showGroups(rawValue);
						}
					}),
					sortable: false,
					editable:false
				}
				/*isEncrypt:{
					label:localization.tt("lbl.encrypted"),
					cell :"Switch",
					editable:false,
				//	canHeaderFilter : true,
				//	headerFilterList :[{label : 'ON',value : 1},{label :'OFF' ,value:2}],
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
					offText : 'OFF'
				},*/
				/*permissions : {
					cell :  "html",
					label : localization.tt("lbl.action"),
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue,model) {
							return '<a href="#!/policy/'+model.id+'" class="btn btn-mini "><i class="icon-edit icon-large" /></a>\
									<a href="javascript:void(0);" data-name ="deletePolicy" data-id="'+model.id+'"  class="btn btn-mini  btn-danger"><i class="icon-trash icon-large" /></a>';
							//You can use rawValue to custom your html, you can change this value using the name parameter.
						}
					}),
					editable:false,

				},
			*/
				
			};
			return this.hdfsResourceList.constructor.getTableCols(cols, this.hdfsResourceList);
		},
		getHiveColumns : function(){
			var that = this;
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
				/*views : {
					label	: localization.tt("lbl.viewName")+'(s)',
					editable:false,
					cell :'string',
					sortable: false,
					// headerSearchStyle  : 'width:85%;',
					//headerFilterList :["Student","Teacher","StudentTeacher"],
					placeholder : 'View Name',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {	return rawValue ? rawValue : '--';}
					}),
				},*/
				udfs : {
					label	: localization.tt("lbl.udfName")+'(s)',
					editable:false,
					cell :'string',
					sortable: false,
					// headerSearchStyle  : 'width:85%;',
					//headerFilterList :["Student","Teacher","StudentTeacher"],
					placeholder : 'UDF Name',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {	return rawValue ? rawValue : '--';}
					})
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
				assetName : {
					label : localization.tt("lbl.repository"),
					cell: "String",
					click : false,
					drag : false,
					editable:false,
					sortable: false
				},
				auditList : {
					label : localization.tt("lbl.auditLogging"),
					cell: "html",
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							//return model.has('auditList') ? 'On' : 'Off';
							return model.has('auditList') ? '<label class="label label-success">ON</label>' : '<label class="label label">OFF</label>';
						}
					}),
					click : false,
					drag : false,
					sortable: false,
					editable:false
				},
				permMapListUser : {
					name : 'permMapList',
					reName : 'userName',
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					label : localization.tt("lbl.users"),
					placeholder : 'User(s)',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							var showMoreLess = false;
							if(_.isArray(rawValue))
								rawValue =  new Backbone.Collection(rawValue);
							if(!_.isUndefined(rawValue) && rawValue.models.length > 0){
								var userArr = _.uniq(_.compact(_.map(rawValue.models, function(m, i){
									if(m.has('userName'))
										return m.get('userName') ;
								})));
								if(userArr.length > 0)
									var resourceId =rawValue.models[0].attributes.resourceId; 
								var newUserArr = _.map(userArr, function(name, i){
									if(i >=  4)
										return '<span class="label label-info" policy-user-id="'+resourceId+'" style="display:none;">' + name + '</span>';
									else if(i == 3 && userArr.length > 4){
										showMoreLess = true;
										return '<span class="label label-info" policy-user-id="'+resourceId+'">' + name + '</span>';
									}
									else
										return '<span class="label label-info" policy-user-id="'+resourceId+'">' + name + '</span>';
								});
								if(showMoreLess){
									newUserArr.push('<span class="pull-left"><a href="javascript:void(0);" data-id="showMoreUsers" class="" policy-user-id="'+resourceId+'"><code> + More..</code></a></span>\
											<span class="pull-left" ><a href="javascript:void(0);" data-id="showLessUsers" class="" policy-user-id="'+resourceId+'" style="display:none;"><code> - Less..</code></a></span>');
								}
								return newUserArr.length ? newUserArr.join(' ') : '--';
							}else
								return '--';
						}
					}),
					editable:false,
					sortable: false
				},
				permMapList : {
					reName : 'groupName',
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					label : localization.tt("lbl.group"),
			//		headerSearchStyle  : 'width:85%;',
					placeholder : 'Group Name',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							return XAUtil.showGroups(rawValue);
						}
					}),
					sortable: false,
					editable:false
				}
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
					offText : 'OFF'
				},*/
				/*permissions : {
					cell :"uri",
					label : localization.tt("lbl.action"),
					href: function(model){
						return '#!/hive/policy/' + model.id;
					},
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							return 'Edit';
							//You can use rawValue to custom your html, you can change this value using the name parameter.
						}
					}),
				//	klass : 'btn btn-mini btn-blue',
					editable:false,
				//	iconKlass :'icon-edit',
				//	iconTitle :'Edit'
					

				}*/
				
				
			};
			return this.hiveResourceList.constructor.getTableCols(cols, this.hiveResourceList);
		},
		getHbaseColumns : function(){
			var cols = {
				policyName : {
					cell : "uri",
					href: function(model){
						return '#!/hbase/'+model.get('assetId')+'/policy/' + model.id;
					},
					label	: localization.tt("lbl.policyName"),
					editable: false,
					sortable : false
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
				columnFamilies : {
					label	: localization.tt("lbl.columnFamilies")+'(s)',
					cell :'html',
					editable:false,
					sortable : false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							return _.isUndefined(rawValue) ? '--': '<span title="'+rawValue+'">'+rawValue+'</span>';  
						}
					})

				},
				assetName : {
					label : localization.tt("lbl.repository"),
					cell: "String",
					click : false,
					drag : false,
					editable:false,
					sortable: false
				},
				auditList : {
					label : localization.tt("lbl.auditLogging"),
					cell: "html",
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							//return model.has('auditList') ? 'On' : 'Off';
							return model.has('auditList') ? '<label class="label label-success">ON</label>' : '<label class="label label">OFF</label>';
						}
					}),
					click : false,
					drag : false,
					sortable: false,
					editable:false
				},
				/*isEncrypt:{
					label:localization.tt("lbl.encrypted"),
					cell :"html",
					editable:false,
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue, model) {
							return model.get('isEncrypt') == XAEnums.BooleanValue.BOOL_TRUE.value ? '<label class="label label-success">ON</label>' : '<label class="label label">OFF</label>';
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
					sortable: false,
					onText : 'ON',
					offText : 'OFF'
				},*/
				permMapListUser : {
					name : 'permMapList',
					reName : 'userName',
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					label : localization.tt("lbl.users"),
					placeholder : 'User(s)',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							var showMoreLess = false;
							if(_.isArray(rawValue))
								rawValue =  new Backbone.Collection(rawValue);
							if(!_.isUndefined(rawValue) && rawValue.models.length > 0){
								var userArr = _.uniq(_.compact(_.map(rawValue.models, function(m, i){
									if(m.has('userName'))
										return m.get('userName') ;
								})));
								if(userArr.length > 0)
									var resourceId =rawValue.models[0].attributes.resourceId; 
								var newUserArr = _.map(userArr, function(name, i){
									if(i >=  4)
										return '<span class="label label-info" policy-user-id="'+resourceId+'" style="display:none;">' + name + '</span>';
									else if(i == 3 && userArr.length > 4){
										showMoreLess = true;
										return '<span class="label label-info" policy-user-id="'+resourceId+'">' + name + '</span>';
									}
									else
										return '<span class="label label-info" policy-user-id="'+resourceId+'">' + name + '</span>';
								});
								if(showMoreLess){
									newUserArr.push('<span class="pull-left"><a href="javascript:void(0);" data-id="showMoreUsers" class="" policy-user-id="'+resourceId+'"><code> + More..</code></a></span>\
											<span class="pull-left" ><a href="javascript:void(0);" data-id="showLessUsers" class="" policy-user-id="'+resourceId+'" style="display:none;"><code> - Less..</code></a></span>');
								}
								return newUserArr.length ? newUserArr.join(' ') : '--';
							}else
								return '--';
						}
					}),
					editable:false,
					sortable: false
				},
				permMapList : {
					reName : 'groupName',
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					label : localization.tt("lbl.group"),
					canHeaderSearch : false,
					// headerSearchStyle  : 'width:70%;',
					placeholder : 'Group Name',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							return XAUtil.showGroups(rawValue);
						}
					}),
					sortable: false,
					editable:false
				}
				
				
			};
			return this.hbaseResourceList.constructor.getTableCols(cols, this.hbaseResourceList);
		},
		getKnoxColumns : function(){
			var cols = {
				policyName : {
					cell : "uri",
					href: function(model){
						return '#!/knox/'+model.get('assetId')+'/policy/' + model.id;
					},
					label	: localization.tt("lbl.policyName"),
					editable: false,
					sortable : false
				},	
				topologies : {
					label	: localization.tt("lbl.topologyName")+'(s)',
					/*href: function(model){
						return '#!/knox/'+model.get('assetId')+'/policy/' + model.id;
					},*/
					editable:false,
					cell :'string',
					sortable : false
						
				},
				services: {
					label	: localization.tt("lbl.serivceName")+'(s)',
					editable:false,
					cell :'string',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {	return rawValue ? rawValue : '--';}
					}),
					sortable : false
				},
				assetName : {
					label : localization.tt("lbl.repository"),
					cell: "String",
					click : false,
					drag : false,
					editable:false,
					sortable: false
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
				permMapListUser : {
					name : 'permMapList',
					reName : 'userName',
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					label : localization.tt("lbl.users"),
					placeholder : 'User(s)',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							var showMoreLess = false;
							if(_.isArray(rawValue))
								rawValue =  new Backbone.Collection(rawValue);
							if(!_.isUndefined(rawValue) && rawValue.models.length > 0){
								var userArr = _.uniq(_.compact(_.map(rawValue.models, function(m, i){
									if(m.has('userName'))
										return m.get('userName') ;
								})));
								if(userArr.length > 0)
									var resourceId =rawValue.models[0].attributes.resourceId; 
								var newUserArr = _.map(userArr, function(name, i){
									if(i >=  4)
										return '<span class="label label-info" policy-user-id="'+resourceId+'" style="display:none;">' + name + '</span>';
									else if(i == 3 && userArr.length > 4){
										showMoreLess = true;
										return '<span class="label label-info" policy-user-id="'+resourceId+'">' + name + '</span>';
									}
									else
										return '<span class="label label-info" policy-user-id="'+resourceId+'">' + name + '</span>';
								});
								if(showMoreLess){
									newUserArr.push('<span class="pull-left"><a href="javascript:void(0);" data-id="showMoreUsers" class="" policy-user-id="'+resourceId+'"><code> + More..</code></a></span>\
											<span class="pull-left" ><a href="javascript:void(0);" data-id="showLessUsers" class="" policy-user-id="'+resourceId+'" style="display:none;"><code> - Less..</code></a></span>');
								}
								return newUserArr.length ? newUserArr.join(' ') : '--';
							}else
								return '--';
						}
					}),
					editable:false,
					sortable: false
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
			};
			return this.knoxResourceList.constructor.getTableCols(cols, this.knoxResourceList);
		},
		getStormColumns : function(){
			var cols = {
				policyName : {
					cell : "uri",
					href: function(model){
						return '#!/knox/'+model.get('assetId')+'/policy/' + model.id;
					},
					label	: localization.tt("lbl.policyName"),
					editable: false,
					sortable : false
				},	
				topologies : {
					label	: localization.tt("lbl.topologyName")+'(s)',
					/*href: function(model){
						return '#!/knox/'+model.get('assetId')+'/policy/' + model.id;
					},*/
					editable:false,
					cell :'string',
					sortable : false
						
				},
				/*services: {
					label	: localization.tt("lbl.serivceName")+'(s)',
					editable:false,
					cell :'string',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {	return rawValue ? rawValue : '--';}
					}),
					sortable : false
				},*/
				assetName : {
					label : localization.tt("lbl.repository"),
					cell: "String",
					click : false,
					drag : false,
					editable:false,
					sortable: false
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
				permMapListUser : {
					name : 'permMapList',
					reName : 'userName',
					cell	: Backgrid.HtmlCell.extend({className: 'cellWidth-1'}),
					label : localization.tt("lbl.users"),
					placeholder : 'User(s)',
					formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
						fromRaw: function (rawValue) {
							var showMoreLess = false;
							if(_.isArray(rawValue))
								rawValue =  new Backbone.Collection(rawValue);
							if(!_.isUndefined(rawValue) && rawValue.models.length > 0){
								var userArr = _.uniq(_.compact(_.map(rawValue.models, function(m, i){
									if(m.has('userName'))
										return m.get('userName') ;
								})));
								if(userArr.length > 0)
									var resourceId =rawValue.models[0].attributes.resourceId; 
								var newUserArr = _.map(userArr, function(name, i){
									if(i >=  4)
										return '<span class="label label-info" policy-user-id="'+resourceId+'" style="display:none;">' + name + '</span>';
									else if(i == 3 && userArr.length > 4){
										showMoreLess = true;
										return '<span class="label label-info" policy-user-id="'+resourceId+'">' + name + '</span>';
									}
									else
										return '<span class="label label-info" policy-user-id="'+resourceId+'">' + name + '</span>';
								});
								if(showMoreLess){
									newUserArr.push('<span class="pull-left"><a href="javascript:void(0);" data-id="showMoreUsers" class="" policy-user-id="'+resourceId+'"><code> + More..</code></a></span>\
											<span class="pull-left" ><a href="javascript:void(0);" data-id="showLessUsers" class="" policy-user-id="'+resourceId+'" style="display:none;"><code> - Less..</code></a></span>');
								}
								return newUserArr.length ? newUserArr.join(' ') : '--';
							}else
								return '--';
						}
					}),
					editable:false,
					sortable: false
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
			};
			return this.stormResourceList.constructor.getTableCols(cols, this.stormResourceList);
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
			var that = this;
			var groups = (this.ui.userGroup.is(':visible')) ? this.ui.userGroup.select2('val'):undefined;
			var users = (this.ui.userName.is(':visible')) ? this.ui.userName.select2('val'):undefined;
			var rxName = this.ui.resourceName.val();
			var policyName = this.ui.policyName.val();
			var params = {groupId : groups,userId : users,name : rxName, policyName : policyName};
			
			this.getResourceLists(params);
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
			var id = $(e.currentTarget).attr('policy-group-id');
			var elem = $(e.currentTarget).closest('.showMoreLess').attr('data-id');
			$('[data-id="'+elem+'"]').find('[policy-group-id="'+id+'"]').show();
			$('[data-id="showLess"][policy-group-id="'+id+'"]').show();
			$('[data-id="showMore"][policy-group-id="'+id+'"]').hide();
		},
		onShowLess : function(e){
			var id = $(e.currentTarget).attr('policy-group-id');
			var elem = $(e.currentTarget).closest('.showMoreLess').attr('data-id')
			$('[data-id="'+elem+'"]').find('[policy-group-id="'+id+'"]').slice(4).hide();
			$('[data-id="showLess"][policy-group-id="'+id+'"]').hide();
			$('[data-id="showMore"][policy-group-id="'+id+'"]').show();
		},
		onShowMoreUsers : function(e){
			var id = $(e.currentTarget).attr('policy-user-id');
			var elem = $(e.currentTarget).closest('.showMoreLess').attr('data-id');
			$('[data-id="'+elem+'"]').find('[policy-user-id="'+id+'"]').show();
			$('[data-id="showLessUsers"][policy-user-id="'+id+'"]').show();
			$('[data-id="showMoreUsers"][policy-user-id="'+id+'"]').hide();
		},
		onShowLessUsers : function(e){
			var id = $(e.currentTarget).attr('policy-user-id');
			var elem = $(e.currentTarget).closest('.showMoreLess').attr('data-id');
			$('[data-id="'+elem+'"]').find('[policy-user-id="'+id+'"]').slice(4).hide();
			$('[data-id="showLessUsers"][policy-user-id="'+id+'"]').hide();
			$('[data-id="showMoreUsers"][policy-user-id="'+id+'"]').show();
		},
		/** on close */
		onClose : function() {
		}
	});

	return UserAccessLayout;
});
