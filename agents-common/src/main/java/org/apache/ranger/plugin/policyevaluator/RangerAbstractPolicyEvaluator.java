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
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.util.ServiceDefUtil;

import java.util.Map;

public abstract class RangerAbstractPolicyEvaluator implements RangerPolicyEvaluator {
	private static final Log LOG = LogFactory.getLog(RangerAbstractPolicyEvaluator.class);

	private   RangerPolicy        policy;
	private   RangerServiceDef    serviceDef;
	private   RangerResourceDef   leafResourceDef;
	private   int                 evalOrder;
	protected RangerPluginContext pluginContext = null;


	public void setPluginContext(RangerPluginContext pluginContext) { this.pluginContext = pluginContext; }

	public RangerPluginContext getPluginContext() { return pluginContext; }

	@Override
	public void init(RangerPolicy policy, RangerServiceDef serviceDef, RangerPolicyEngineOptions options) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAbstractPolicyEvaluator.init(" + policy + ", " + serviceDef + ")");
		}

		this.policy          = policy;
		this.serviceDef      = serviceDef;
		this.leafResourceDef = ServiceDefUtil.getLeafResourceDef(serviceDef, getPolicyResource());

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
	public int getPolicyPriority() {
		return policy != null && policy.getPolicyPriority() != null ? policy.getPolicyPriority() : RangerPolicy.POLICY_PRIORITY_NORMAL;
	}

	@Override
	public RangerServiceDef getServiceDef() {
		return serviceDef;
	}

	@Override
	public boolean isAncestorOf(RangerResourceDef resourceDef) {
		return ServiceDefUtil.isAncestorOf(serviceDef, leafResourceDef, resourceDef);
	}

	public boolean hasAllow() {
		return policy != null && CollectionUtils.isNotEmpty(policy.getPolicyItems());
	}

	protected boolean hasMatchablePolicyItem(RangerAccessRequest request) {
		return hasAllow() || hasDeny();
	}

	public boolean hasDeny() {
		return policy != null && (policy.getIsDenyAllElse() || CollectionUtils.isNotEmpty(policy.getDenyPolicyItems()));
	}

	@Override
	public int getEvalOrder() {
		return evalOrder;
	}
	@Override
	public boolean isAuditEnabled() {
		return policy != null && policy.getIsAuditEnabled();
	}

	public void setEvalOrder(int evalOrder) {
		this.evalOrder = evalOrder;
	}

	@Override
	public PolicyACLSummary getPolicyACLSummary() { return null; }

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerAbstractPolicyEvaluator={");

		sb.append("policy={");
		if (policy != null) {
			policy.toString(sb);
		}
		sb.append("} ");

		sb.append("}");

		return sb;
	}
}
