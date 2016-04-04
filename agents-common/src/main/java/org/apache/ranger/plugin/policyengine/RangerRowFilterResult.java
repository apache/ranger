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
package org.apache.ranger.plugin.policyengine;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;


public class RangerRowFilterResult extends RangerAccessResult {
	private String filterExpr = null;


	public RangerRowFilterResult(final String serviceName, final RangerServiceDef serviceDef, final RangerAccessRequest request) {
		this(serviceName, serviceDef, request, null);
	}

	public RangerRowFilterResult(final String serviceName, final RangerServiceDef serviceDef, final RangerAccessRequest request, final RangerPolicy.RangerPolicyItemRowFilterInfo rowFilterInfo) {
		super(serviceName, serviceDef, request);

		if(rowFilterInfo != null) {
			setFilterExpr(rowFilterInfo.getFilterExpr());
		}
	}

	/**
	 * @return the filterExpr
	 */
	public String getFilterExpr() {
		return filterExpr;
	}

	/**
	 * @param filterExpr the filterExpr to set
	 */
	public void setFilterExpr(String filterExpr) {
		this.filterExpr = filterExpr;
	}

	public boolean isRowFilterEnabled() {
		return StringUtils.isNotEmpty(filterExpr);
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerRowFilterResult={");

		super.toString(sb);

		sb.append("filterExpr={").append(filterExpr).append("} ");

		sb.append("}");

		return sb;
	}
}
