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
 * Policy Conditions view
 */

define(function(require) {
	'use strict';

	var Backbone = require('backbone');
	var XAUtil = require('utils/XAUtils');
	var localization = require('utils/XALangSupport');

	var RangerPolicyConditionsTmpl = require('hbs!tmpl/policies/RangerPolicyConditions_tmpl');

	var RangerPolicyConditions = Backbone.Marionette.Layout.extend({
		_viewName: 'RangerPolicyConditions',

		template: RangerPolicyConditionsTmpl,

		templateHelpers: function() {
			return {
				policyItems: this.policyItems,
				isMaskingPolicy: XAUtil.isMaskingPolicy(this.policyId),
				isAccessPolicy: XAUtil.isAccessPolicy(this.policyId),
				isRowFilterPolicy: XAUtil.isRowFilterPolicy(this.policyId)
			};
		},

		initialize: function(options) {
			console.log("initialized a RangerPolicyConditions Layout");
			_.extend(this, _.pick(options, 'policyItems', 'policyId'));
		}

	});
	return RangerPolicyConditions;
});