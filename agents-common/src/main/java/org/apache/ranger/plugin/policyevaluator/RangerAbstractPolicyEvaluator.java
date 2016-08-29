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



import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceEvaluator;
import org.apache.ranger.plugin.util.ServiceDefUtil;


import java.util.Map;


public abstract class RangerAbstractPolicyEvaluator implements RangerPolicyEvaluator {
	private static final Log LOG = LogFactory.getLog(RangerAbstractPolicyEvaluator.class);

	private RangerPolicy     policy            = null;
	private RangerServiceDef serviceDef        = null;
	private Integer          leafResourceLevel = null;
	private int              evalOrder         = 0;


	@Override
	public void init(RangerPolicy policy, RangerServiceDef serviceDef, RangerPolicyEngineOptions options) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAbstractPolicyEvaluator.init(" + policy + ", " + serviceDef + ")");
		}

		this.policy            = policy;
		this.serviceDef        = serviceDef;
		this.leafResourceLevel = ServiceDefUtil.getLeafResourceLevel(serviceDef, getPolicyResource());

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAbstractPolicyEvaluator.init(" + policy + ", " + serviceDef + ")");
		}
	}

	@Override
	public long getId() {
		return policy != null ? policy.getId() :-1;
	}

	@Override
	public Map<String, RangerPolicy.RangerPolicyResource> getPolicyResource() {
		return policy !=null ? policy.getResources() : null;
	}

	@Override
	public RangerPolicy getPolicy() {
		return policy;
	}

	@Override
	public RangerServiceDef getServiceDef() {
		return serviceDef;
	}

	@Override
	public Integer getLeafResourceLevel() {
		return leafResourceLevel;
	}


	@Override
	public int getEvalOrder() {
		return evalOrder;
	}

	@Override
	public boolean isAuditEnabled() {
		return policy != null && policy.getIsAuditEnabled();
	}

	@Override
	public int compareTo(RangerPolicyResourceEvaluator obj) {
		if(LOG.isDebugEnabled()) {
		LOG.debug("==> RangerAbstractPolicyEvaluator.compareTo()");
		}

		int result;

		if(obj instanceof RangerPolicyEvaluator) {
			RangerPolicyEvaluator other = (RangerPolicyEvaluator)obj;

			result = Integer.compare(this.getEvalOrder(), other.getEvalOrder());

			if (result == 0) {
				result = Integer.compare(getCustomConditionsCount(), other.getCustomConditionsCount());
			}
		} else {
			result = Long.compare(getId(), obj.getId());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAbstractPolicyEvaluator.compareTo(), result:" + result);
		}

		return result;
	}

	public void setEvalOrder(int evalOrder) {
		this.evalOrder = evalOrder;
	}

	public boolean hasAllow() {
		return policy != null && CollectionUtils.isNotEmpty(policy.getPolicyItems());
	}

	protected boolean hasMatchablePolicyItem(RangerAccessRequest request) {
		return hasAllow();
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerAbstractPolicyEvaluator={");

		sb.append("policy={").append(policy).append("} ");
		sb.append("serviceDef={").append(serviceDef).append("} ");

		sb.append("}");

		return sb;
	}
}
