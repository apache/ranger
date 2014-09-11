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
	var Communicator	= require('communicator');
	var XAUtil			= require('utils/XAUtils');
	var XAEnums			= require('utils/XAEnums');
	var localization	= require('utils/XALangSupport');
	var VXGroup			= require('models/VXGroup');
	var AddGroup_tmpl = require('hbs!tmpl/common/AddGroup_tmpl');
	
	require('bootstrap-editable');
	var AddGroup = Backbone.Marionette.ItemView.extend(
	/** @lends AddGroup */
	{
		_viewName : 'AddGroup',
		
    	template: AddGroup_tmpl,
        templateHelpers :function() {
        	return {
        		groupId : this.model.get('groupIdList'),
        		isModelNew	: this.model.isNew()
        	};
        },
    	/** ui selector cache */
    	ui: {
    		'addGroup' :'#addGroup',
    		'errorMsg' : '[data-error="groupIdList"]'
    	},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			return events;
		},

    	/**
		* intialize a new AddGroup ItemView 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a AddGroup ItemView");

			_.extend(this, _.pick(options, 'groupList'));
			this.bindEvents();
		},

		/** all events binding here */
		bindEvents : function(){
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
		},

		/** on render callback */
		onRender: function() {
			var that = this , arr =[];
			this.groupArr = this.groupList.map(function(m){
				return { id : m.id+"" , text : m.get('name')};
			});
			this.initializePlugins();
			$.fn.editable.defaults.mode = 'popover';
			
			if(!_.isUndefined(this.model.get('userSource')) && this.model.get('userSource') == XAEnums.UserSource.XA_USER.value){
				this.$el.find('#tags-edit-1').hide();
				var labelStr1 = this.$el.find('label').html().replace('*','');
				this.$el.find('label').html(labelStr1);
			}

			this.$('.tags').editable({
			    placement: 'right',
			    emptytext : 'Please select',
				select2 :this.getSelect2Options(),
			    display: function(values,srcDate) {
			    	if(_.isNull(values)){
			    		$(this).html('');
			    		return;
			    	}
			    	that.checkDirtyFieldForGroup(values);
			    	if(!_.isArray(values))
			    		values=values.toString().split(',');
			    	var valArr = [];
		    		var valArr = _.map(values, function(val){
		    			var obj = _.findWhere(that.groupArr,{id:val});
		    			return "<span class='label label-inverse'>" + obj.text + "</span>";
		    		});
		    		
		    		$(this).html(valArr.join(" "));
		    		if(valArr.length > 0){
		    			that.$('.field-groupIdList').removeClass('error');
		    			that.ui.errorMsg.hide();
		    		}else{
		    			that.$('.field-groupIdList').addClass('error');
		    			that.ui.errorMsg.show();
		    		}
			    		
			    },
			    success: function(response, newValue) {
			    	console.log(newValue);
			    	//that.model.set('group',newValue);
			    	
			    }
			});
			this.$('[id^="tags-edit-"]').click(function(e) {
			    e.stopPropagation();
			    e.preventDefault();
			    $('#' + $(this).data('editable') ).editable('toggle');
			});

		},
		/** all post render plugin initialization */
		initializePlugins: function(){
		},
		checkDuplicatesInArray :function(array) {
		    var valuesSoFar = {};
		    for (var i = 0; i < array.length; ++i) {
		        var value = array[i];
		        if (Object.prototype.hasOwnProperty.call(valuesSoFar, value)) {
		            return true;
		        }
		        valuesSoFar[value] = true;
		    }
		    return false;
		},
		getSelect2Options :function(){
			var that = this;
			return{
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
				/*createSearchChoice: function(term, data) {
					if ($(data).filter(function() {
						return this.text.localeCompare(term) === 0;
					}).length === 0) {
						return {
							id : term,
							text: term
						};
					}
				},*/
				ajax: { 
					url: "service/xusers/groups",
					dataType: 'json',
					data: function (term, page) {
						return {name : term};
					},
					results: function (data, page) { 
						var results = [],selectedVals = [];
						if(!_.isEmpty(that.$('.tags').data('editable').input.$input.val()))
							selectedVals = that.$('.tags').data('editable').input.$input.val().split(',');
						if(data.resultSize != "0"){
							//if(data.vXGroups.length > 1){
								results = data.vXGroups.map(function(m, i){	return {id : (m.id).toString(), text: m.name};	});
								if(!_.isEmpty(selectedVals))
									results = XAUtil.filterResultByIds(results, selectedVals);
				//				console.log(results.length);
								return {results : results};
					//		}
						//	results = [{id : (data.vXGroups.id)+"", text: data.vXGroups.name}];
						//	return {results : results};
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
			};
		},
		checkDirtyFieldForGroup : function(changeValues){
			var groupIdList = [];
			if(!_.isArray(changeValues))
				changeValues = [changeValues];
			changeValues = _.map(changeValues, function(val){return parseInt(val);});
			if(!_.isUndefined(this.model.get('groupIdList')))
				groupIdList = this.model.get('groupIdList'); 
			XAUtil.checkDirtyField(groupIdList, changeValues, this.$el);
		},
		
		/** on close */
		onClose: function(){
		}

	});

	return AddGroup;
});
