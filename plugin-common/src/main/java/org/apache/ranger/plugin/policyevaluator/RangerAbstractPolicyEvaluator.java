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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;


public abstract class RangerAbstractPolicyEvaluator implements RangerPolicyEvaluator {
	private static final Log LOG = LogFactory.getLog(RangerAbstractPolicyEvaluator.class);

	private RangerPolicy     policy     = null;
	private RangerServiceDef serviceDef = null;


	@Override
	public void init(RangerPolicy policy, RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAbstractPolicyEvaluator.init(" + policy + ", " + serviceDef + ")");
		}

		this.policy     = policy;
		this.serviceDef = serviceDef;

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAbstractPolicyEvaluator.init(" + policy + ", " + serviceDef + ")");
		}
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
