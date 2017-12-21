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

/*
 * Policy Detail view
 */

define(function(require) {
	'use strict';

	var Backbone = require('backbone');
	var XAUtil = require('utils/XAUtils');
	var localization = require('utils/XALangSupport');
	var RangerServiceDef = require('models/RangerServiceDef');
	var RangerPolicyDetailTmpl = require('hbs!tmpl/policies/RangerPolicyDetail_tmpl');
	var RangerPolicyConditions = require('views/policies/RangerPolicyConditions');

	var RangerPolicyDetail = Backbone.Marionette.Layout.extend({
		_viewName: 'RangerPolicyDetail',

		template: RangerPolicyDetailTmpl,

		regions: {
			'allowCondition': 'div[data-id="allowCondition"]',
			'exAllowCondition': 'div[data-id="exAllowCondition"]',
			'denyCondition': 'div[data-id="denyCondition"]',
			'exDenyCondition': 'div[data-id="exDenyCondition"]'
		},

		templateHelpers: function() {
			var policyId = this.model.get("policyType");
			var policyType = localization.tt('h.access');
			var conditionType = localization.tt('lbl.allow');
			if(XAUtil.isMaskingPolicy(policyId)) {
				policyType = localization.tt('h.masking');
				conditionType = localization.tt('h.mask');
			} else if(XAUtil.isRowFilterPolicy(policyId)) {
				policyType = localization.tt('lbl.rowLevelFilter');
				conditionType = localization.tt('h.rowFilter');
			}
			return {
				policyType: policyType,
				resources: this.initResource(),
				conditionType: conditionType,
				isMaskingPolicy: XAUtil.isMaskingPolicy(policyId),
				isAccessPolicy: XAUtil.isAccessPolicy(policyId),
				isRowFilterPolicy: XAUtil.isRowFilterPolicy(policyId)
			};
		},

		initialize: function(options) {
			console.log("initialized a RangerPolicyDetail Layout");
			_.extend(this, _.pick(options, 'rangerService'));
			this.initializeServiceDef();
		},

		initializeServiceDef: function() {
			this.rangerServiceDefModel = new RangerServiceDef();
			this.rangerServiceDefModel.url = XAUtil.getRangerServiceDef(this.rangerService.get('type'));
			this.rangerServiceDefModel.fetch({
				cache: false,
				async: false
			});
		},

		initResource: function() {
			var resources = [];
			var resourceObj = this.model.get("resources");
			var configs = this.rangerServiceDefModel.get("resources")
			if(XAUtil.isMaskingPolicy(this.model.get('policyType'))) {
				if(XAUtil.isRenderMasking(this.rangerServiceDefModel.get('dataMaskDef'))) {
					configs = this.rangerServiceDefModel.get('dataMaskDef').resources;
				}
			} else if(XAUtil.isRowFilterPolicy(this.model.get('policyType'))) {
				if(XAUtil.isRenderRowFilter(this.rangerServiceDefModel.get('rowFilterDef'))) {
					configs = this.rangerServiceDefModel.get('rowFilterDef').resources;
				}
			}
			_.each(configs, function(obj, index) {
				var value = resourceObj[obj.name];
				if(_.isUndefined(value)){
					return;
				}
				var resource = {};
				resource.label = obj.label;
				resource.values = value.values;
				resource.isSupport = false;
				if(obj.excludesSupported){
					resource.isSupport = true;
					if(value.isExcludes){
						resource.exBool = false;
						resource.exLabel = localization.tt('h.exclude')
					}else{
						resource.exBool = true;
						resource.exLabel = localization.tt('h.include')
					}
				}else if(obj.recursiveSupported){
					resource.isSupport = true;
					if(value.isRecursive){
						resource.exBool = true;
						resource.exLabel = localization.tt('h.recursive')
					}else{
						resource.exBool = fasle;
						resource.exLabel = localization.tt('h.nonRecursive')
					}
				}
				resources.push(resource);
			});
			return resources;
		},

		onRender: function() {
			this.initializeConditions();
		},

		initializeConditions: function() {
			var policyId = this.model.get("policyType");
			if(XAUtil.isAccessPolicy(policyId)) {
				this.allowCondition.show(new RangerPolicyConditions({
					policyItems: this.model.get("policyItems"),
					policyId: policyId
				}));
				this.exAllowCondition.show(new RangerPolicyConditions({
					policyItems: this.model.get("allowExceptions"),
					policyId: policyId
				}));
				this.denyCondition.show(new RangerPolicyConditions({
					policyItems: this.model.get("denyPolicyItems"),
					policyId: policyId
				}));
				this.exDenyCondition.show(new RangerPolicyConditions({
					policyItems: this.model.get("denyExceptions"),
					policyId: policyId
				}));
			} else if(XAUtil.isMaskingPolicy(policyId)) {
				this.allowCondition.show(new RangerPolicyConditions({
					policyItems: this.model.get("dataMaskPolicyItems"),
					policyId: policyId
				}));
			} else if(XAUtil.isRowFilterPolicy(policyId)) {
				this.allowCondition.show(new RangerPolicyConditions({
					policyItems: this.model.get("rowFilterPolicyItems"),
					policyId: policyId
				}));
			}
		}

	});
	return RangerPolicyDetail;
});