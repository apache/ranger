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

	var Backbone						= require('backbone');
	var XAEnums					 		= require('utils/XAEnums');
	var XALinks							= require('modules/XALinks');
	var XAUtils					 		= require('utils/XAUtils');
	var RangerPolicy					= require('models/RangerPolicy');
	var RangerService					= require('models/RangerService');
	var RangerServiceDef				= require('models/RangerServiceDef');
	var PolicyOperationDiff_tmpl 		= require('hbs!tmpl/reports/PlugableServicePolicyDiff_tmpl');
	var PolicyUpdateOperationDiff_tmpl 	= require('hbs!tmpl/reports/PlugableServicePolicyUpdateDiff_tmpl');
	var PolicyDeleteOperationDiff_tmpl 	= require('hbs!tmpl/reports/PlugableServicePolicyDeleteDiff_tmpl');
	var PolicyDeleteUpdateOperationDiff_tmpl 	= require('hbs!tmpl/reports/PolicyDeleteOperationDiff_tmpl');
	
	var PlugableServiceDiffDetail = Backbone.Marionette.ItemView.extend(
	/** @lends PlugableServiceDiffDetail */
	{
		_viewName : 'PlugableServiceDiffDetail',
		
    	template: PolicyOperationDiff_tmpl,
        templateHelpers :function(){
        	return {
        			collection : this.collection.models,
        			action	   : this.action,
        			objectName : this.objectName,
        			objectId   : this.objectId,
        			objectCreatedDate : this.objectCreatedDate,
        			objectCreatedBy : this.objectCreatedBy,
        			newPolicyItems : this.newPermList,
					oldPolicyItems : this.oldPermList,
					policyName	 : this.policyName,
					policyId	 : this.policyId,
					repositoryType : this.repositoryType,
        			
        		};
        },
    	/** ui selector cache */
    	ui: {
    		groupPerm : '.groupPerm',
    		userPerm  : '.userPerm',
    		oldValues : '[data-id="oldValues"]',
    		diff 	  : '[data-id="diff"]',
    		policyDiff: '[data-name="policyDiff"]',
    		policyDetail: '[class="policyDetail"]'
    		
    	},

		/** ui events hash */
		events: function() {
			var events = {};
			//events['change ' + this.ui.input]  = 'onInputChange';
			return events;
		},

    	/**
		* intialize a new PlugableServiceDiffDetail ItemView 
		* @constructs
		*/
		initialize: function(options) {
			console.log("initialized a PlugableServiceDiffDetail ItemView");
			
			_.extend(this, _.pick(options, 'classType','objectName','objectId','objectCreatedDate','action','userName','policyId'));
			this.bindEvents();
			//this.initializeDiffOperation();
			this.initializeServiceDef();
			this.getTemplateForView();
			
		},
		initializeServiceDef : function(){
			var url;
			/*if(this.action == 'update'){
				var rangerPolicy = new RangerPolicy({'id':this.objectId})
				rangerPolicy.fetch({
					cache : false,
					async : false
				})
				this.policyName = rangerPolicy.get('name');
				url = XAUtils.getRangerServiceByName(rangerPolicy.get('service'))
			}else{
			}*/
			var policyName = this.collection.findWhere({'attributeName':'Policy Name'});
			if(this.action == 'create'){
				this.policyName = policyName.get('newValue');
			}else if(this.action == 'delete'){
				this.policyName = policyName.get('previousValue');
			}
			if(!_.isUndefined(this.collection.models[0]) ){
				this.policyName = _.isUndefined(this.policyName) ? this.collection.models[0].get('objectName') : this.policyName;
				if(this.action != 'delete'){
					url = XAUtils.getRangerServiceByName(this.collection.models[0].get('parentObjectName'))
					var rangerService = new RangerService()
					rangerService.url = url;
					rangerService.fetch({
						cache : false,
						async : false
					})
					this.rangerServiceDefModel = new RangerServiceDef();
					this.rangerServiceDefModel.url = XAUtils.getRangerServiceDef(rangerService.get('type'));
					this.rangerServiceDefModel.fetch({
						cache : false,
						async : false
					})
					this.repositoryType = this.rangerServiceDefModel.get('name');
				}
				
				//get policy created/updated date/owner
				var model = this.collection.models[0];
				this.objectCreatedBy = model.get('updatedBy');
			}
		},
		/** all events binding here */
		bindEvents : function(){
			/*this.listenTo(this.model, "change:foo", this.modelChanged, this);*/
			/*this.listenTo(communicator.vent,'someView:someEvent', this.someEventHandler, this)'*/
		},
		/** on render callback */
		onRender: function() {
			this.initializePlugins();
			this.removeLastCommaFromSpans(this.$el.find('.policyDetail').find('ol li'))
			this.removeLastCommaFromSpans(this.ui.diff.find('ol li'))
			
			_.each(this.ui.policyDiff.find('ol li'),function(m){
				if(_.isEmpty($(m).text().trim()))
					$(m).removeClass('change-row').text('--');
			});
			//Remove last br from ol
			this.$el.find('.diff-perms').find('.diff-right').find('ol:last').next().remove()
			this.$el.find('.diff-perms').find('.diff-left').find('ol:last').next().remove()
			
			var newOl = this.$el.find('.diff-perms').find('.diff-right').find('ol');
			var oldOl = this.$el.find('.diff-perms').find('.diff-left').find('ol');
			
			_.each(oldOl, function(ol, i) {
				console.log()
				this.highLightElement($(ol).find('.username'), $(newOl[i]).find('.username'));
				this.highLightElement($(ol).find('.groupname'), $(newOl[i]).find('.groupname'));
				this.highLightElement($(ol).find('.perm'), $(newOl[i]).find('.perm'));
				this.highLightElement($(ol).find('.condition'), $(newOl[i]).find('.condition'));
				
			},this);
		},
		removeLastCommaFromSpans : function($el) {
			//remove last comma
			_.each($el,function(m){
				var text = $(m).text().replace(/,(?=[^,]*$)/, '');
				$(m).find('span').last().remove();
			});
		},
		highLightElement : function(oldOlList, newOlList) {
			var removedUsers = this.array_diff(oldOlList, newOlList)
			var addedUsers = this.array_diff(newOlList, oldOlList)
			_.each(removedUsers, function(userSpan) { $(userSpan).addClass('delete-text')});
			_.each(addedUsers, function(userSpan) { $(userSpan).addClass('add-text')});
		},
		array_diff :function(array1, array2){
//			var array1 = [<span>user1<span>,<span>user2<span>,<span>user3<span>,<span>user4<span>];
//			var array2 = [<span>user1<span>,<span>user3<span>];
//			array_diff = [<span>user2<span>,<span>user4<span>]
			var difference = [];
			var tmpArr2 = _.map(array2,function(a){ return (a.innerHTML);})
			$.grep(array1, function(el) {
			        if ($.inArray(el.innerHTML, tmpArr2) == -1){
			        	difference.push(el);
			        } 
			        	
			});
			return difference;
		},
		getTemplateForView : function(){
			if(this.action == 'create'){
				this.template = PolicyOperationDiff_tmpl;
			}else if(this.action == 'update'){
				this.template = PolicyUpdateOperationDiff_tmpl;
			}else{
				this.template = PolicyDeleteOperationDiff_tmpl;
			}
			//prepare data for template
			this.newPolicyItems = null, this.oldPolicyItems = null ;
			var policyStatus = this.collection.findWhere({'attributeName':'Policy Status'})
			if(!_.isUndefined(policyStatus)){
				if(!_.isEmpty(policyStatus.get('previousValue'))){
					var tmp = this.collection.get(policyStatus.id)
					tmp.set("previousValue", policyStatus.get('previousValue') == "true" ? 'enabled' : 'disabled')
				}
				if(!_.isEmpty(policyStatus.get('newValue'))){
					var tmp = this.collection.get(policyStatus.id)
					tmp.set("newValue", policyStatus.get('newValue') ==  "true" ? 'enabled' : 'disabled')
				}
			}
			var policyResource = this.collection.findWhere({'attributeName':'Policy Resources'})
			if(!_.isUndefined(policyResource)){
				this.getPolicyResources();
			}
			var policyItems = this.collection.findWhere({'attributeName':'Policy Items'});
			if(!_.isUndefined(policyItems)){
				this.getPolicyItems();
			}
		},
		getPolicyResources : function() {
			var policyResources = this.collection.findWhere({'attributeName':'Policy Resources'});
			this.collection.remove(policyResources);
			if(!_.isUndefined(policyResources.get('newValue')) && !_.isEmpty(policyResources.get('newValue'))){
				var resources = {} ;
				var resourceNewValues = JSON.parse(policyResources.get('newValue'));
				if(!_.isUndefined(this.rangerServiceDefModel)){
					_.each(this.rangerServiceDefModel.get('resources'), function(obj) {
						_.each(resourceNewValues,function(val,key){ 
							if(obj.name == key){
								resources[obj.name] = val.values.toString();
								if(!_.isUndefined(obj.excludesSupported) && obj.excludesSupported){
									resources[obj.name+' exclude'] = val.isExcludes.toString();
								}
								if(!_.isUndefined(obj.recursiveSupported) && obj.recursiveSupported){
									resources[obj.name+' recursive'] = val.isRecursive.toString();
								}
							}
						});
					});
				}
			}
			if(!_.isUndefined(policyResources.get('previousValue')) && !_.isEmpty(policyResources.get('previousValue'))){
				var oldResources = {} ;
				var resourceNewValues = JSON.parse(policyResources.get('previousValue'));
				if(!_.isUndefined(this.rangerServiceDefModel)){
					_.each(this.rangerServiceDefModel.get('resources'), function(obj) {
						_.each(resourceNewValues,function(val,key){ 
							if(obj.name == key){
								oldResources[obj.name] = val.values.toString();
								if(!_.isUndefined(obj.excludesSupported) && obj.excludesSupported){
									oldResources[obj.name+' exclude'] = val.isExcludes.toString();
								}
								if(!_.isUndefined(obj.recursiveSupported) && obj.recursiveSupported){
									oldResources[obj.name+' recursive'] = val.isRecursive.toString();
								}
							}
						});
					});
				}
			}
			if(this.action == "update"){
				_.each(resources,function(val, key){ 
//					console.log(oldResources)
					if(val != oldResources[key])
						this.collection.add({'attributeName':key, 'newValue':val.toString(),'previousValue': oldResources[key],type : "Policy Resources"}); 
				}, this)
			}else if(this.action == "create"){
				_.each(resources,function(val, key){ this.collection.add({'attributeName':key, 'newValue':val.toString()}); }, this)
			}else{
				_.each(oldResources,function(val, key){ this.collection.add({'attributeName':key, 'previousValue':val.toString()}); }, this)
			}
		},
		getPolicyItems : function() {
			var items = {};
			var policyItems = this.collection.findWhere({'attributeName':'Policy Items'});
			this.collection.remove(policyItems);
			if(!_.isUndefined(policyItems.get('newValue')) && !_.isEmpty(policyItems.get('newValue'))){
				this.newPolicyItems = JSON.parse(policyItems.get('newValue'));
				_.each(this.newPolicyItems, function(obj){
					if(!_.isUndefined(obj.accesses)){
						var permissions = _.map(_.where(obj.accesses,{'isAllowed':true}), function(t) { return t.type; });
						obj['permissions'] = permissions;
						obj['delegateAdmin'] = obj.delegateAdmin ? 'enabled' : 'disabled';
					}
				});
			}
			if(!_.isUndefined(policyItems.get('previousValue')) && !_.isEmpty(policyItems.get('previousValue'))){
				this.oldPolicyItems = JSON.parse(policyItems.get('previousValue'));
				_.each(this.oldPolicyItems, function(obj){
					if(!_.isUndefined(obj.accesses)){
						var permissions = _.map(_.where(obj.accesses,{'isAllowed':true}), function(t) { return t.type; });
						obj['permissions'] = permissions;
						obj['delegateAdmin'] = obj.delegateAdmin ? 'enabled' : 'disabled';
					}
				});
			}
			this.oldPermList =[], this.newPermList =[]
			if(this.action == "update"){
				this.setOldeNewPermList();
			}else{
				this.oldPermList = this.oldPolicyItems;
				this.newPermList = this.newPolicyItems;
			}
		},
		setOldeNewPermList : function() {
			var found = false;
			for(var i=0; i< this.newPolicyItems.length ;i++){
				found = false;
				for(var j=0; j< this.oldPolicyItems.length ;j++){
					if(!found)
						if(_.intersection(this.oldPolicyItems[j].users,this.newPolicyItems[i].users).length > 0
								|| _.intersection(this.oldPolicyItems[j].groups,this.newPolicyItems[i].groups).length > 0){
							if(JSON.stringify(this.newPolicyItems[i]) != JSON.stringify(this.oldPolicyItems[j])){
								this.oldPermList.push(this.oldPolicyItems[j])
								this.newPermList.push(this.newPolicyItems[i])
							}
							found = true;
						}
				}
				if(!found){
					this.oldPermList.push({})
					this.newPermList.push(this.newPolicyItems[i])
				}
			}
			for(var i=0; i< this.oldPolicyItems.length ;i++){
				found = false;
				for(var j=0; j < this.newPolicyItems.length;j++){
					if(!found && _.intersection(this.oldPolicyItems[i].users,this.newPolicyItems[j].users).length > 0
							|| _.intersection(this.oldPolicyItems[i].groups,this.newPolicyItems[j].groups).length > 0){
						if(JSON.stringify(this.oldPolicyItems[i]) != JSON.stringify(this.newPolicyItems[j])){
							if($.inArray(this.oldPolicyItems[i], this.oldPermList) < 0){
								this.oldPermList.push(this.oldPolicyItems[i])
								this.newPermList.push(this.newPolicyItems[j])
							}
						}
						found = true;
					}
				}
				if(!found){
					this.oldPermList.push(this.oldPolicyItems[i])
					this.newPermList.push({})
				}
			}
			console.log(this.oldPermList)
			console.log(this.newPermList)
		},
		/** all post render plugin initialization */
		initializePlugins: function(){
		},
		/** on close */
		onClose: function(){
		}

	});

	return PlugableServiceDiffDetail;
});
