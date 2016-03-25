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
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerDataMaskTypeDef;
import org.apache.ranger.plugin.util.ServiceDefUtil;


public class RangerDataMaskResult extends RangerAccessResult {
	private String maskType      = null;
	private String maskCondition = null;
	private String maskedValue   = null;


	public RangerDataMaskResult(final String serviceName, final RangerServiceDef serviceDef, final RangerAccessRequest request) {
		this(serviceName, serviceDef, request, null);
	}

	public RangerDataMaskResult(final String serviceName, final RangerServiceDef serviceDef, final RangerAccessRequest request, final RangerPolicyItemDataMaskInfo dataMaskInfo) {
		super(serviceName, serviceDef, request);

		if(dataMaskInfo != null) {
			setMaskType(dataMaskInfo.getDataMaskType());
			setMaskCondition(dataMaskInfo.getConditionExpr());
			setMaskedValue(dataMaskInfo.getValueExpr());
		}
	}

	/**
	 * @return the maskType
	 */
	public String getMaskType() {
		return maskType;
	}

	/**
	 * @param maskType the maskType to set
	 */
	public void setMaskType(String maskType) {
		this.maskType = maskType;
	}

	/**
	 * @return the maskCondition
	 */
	public String getMaskCondition() {
		return maskCondition;
	}

	/**
	 * @param maskCondition the maskCondition to set
	 */
	public void setMaskCondition(String maskCondition) {
		this.maskCondition = maskCondition;
	}

	/**
	 * @return the maskedValue
	 */
	public String getMaskedValue() {
		return maskedValue;
	}
	/**
	 * @param maskedValue the maskedValue to set
	 */
	public void setMaskedValue(String maskedValue) {
		this.maskedValue = maskedValue;
	}

	public boolean isMaskEnabled() {
		return StringUtils.isNotEmpty(this.getMaskType());
	}

	public RangerDataMaskTypeDef getMaskTypeDef() {
		RangerDataMaskTypeDef ret = null;

		if(StringUtils.isNotEmpty(maskType)) {
			ret = ServiceDefUtil.getDataMaskType(getServiceDef(), maskType);
		}

		return ret;
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerDataMaskResult={");

		super.toString(sb);

        sb.append("maskType={").append(maskType).append("} ");
        sb.append("maskCondition={").append(maskCondition).append("} ");
		sb.append("maskedValue={").append(maskedValue).append("} ");

		sb.append("}");

		return sb;
	}
}
