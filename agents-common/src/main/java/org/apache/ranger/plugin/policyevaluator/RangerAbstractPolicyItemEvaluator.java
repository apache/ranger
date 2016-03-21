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
package org.apache.ranger.plugin.policyevaluator;


import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.plugin.conditionevaluator.RangerConditionEvaluator;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyengine.RangerResourceAccessInfo;


public abstract class RangerAbstractPolicyItemEvaluator implements RangerPolicyItemEvaluator {
	final RangerPolicyEngineOptions options;
	final RangerServiceDef          serviceDef;
	final RangerPolicy              policy;
	final RangerPolicyItem          policyItem;
	final long                      policyId;

	List<RangerConditionEvaluator> conditionEvaluators = Collections.<RangerConditionEvaluator>emptyList();

	RangerAbstractPolicyItemEvaluator(RangerServiceDef serviceDef, RangerPolicy policy, RangerPolicyItem policyItem, RangerPolicyEngineOptions options) {
		this.serviceDef = serviceDef;
		this.policy     = policy;
		this.policyItem = policyItem;
		this.options    = options;
		this.policyId   = policy != null && policy.getId() != null ? policy.getId() : -1;
	}

	@Override
	public List<RangerConditionEvaluator> getConditionEvaluators() {
		return conditionEvaluators;
	}

	protected String getServiceType() {
		return serviceDef != null ? serviceDef.getName() : null;
	}

	protected boolean getConditionsDisabledOption() {
		return options != null ? options.disableCustomConditions : false;
	}

	@Override
	public void getResourceAccessInfo(RangerResourceAccessInfo result) {
		if(policyItem != null && result != null) {
			if(CollectionUtils.isNotEmpty(policyItem.getUsers())) {
				result.getAllowedUsers().addAll(policyItem.getUsers());
			}

			if(CollectionUtils.isNotEmpty(policyItem.getGroups())) {
				result.getAllowedGroups().addAll(policyItem.getGroups());
			}
		}
	}
}
