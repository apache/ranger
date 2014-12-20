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

import java.util.ArrayList;
import java.util.List;

import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;

public class RangerDefaultResourceMatcher implements RangerResourceMatcher {
	private List<String> policyValues      = null;
	private boolean      policyIsExcludes  = false;
	private boolean      optIgnoreCase     = false;
	private boolean      optWildCard       = false;

	@Override
	public void init(RangerPolicyResource policyResource, String options) {
		this.policyValues      = new ArrayList<String>();
		this.policyIsExcludes  = false;
		this.optIgnoreCase     = false;
		this.optWildCard       = false;

		if(options != null) {
			for(String optionStr : options.split(OPTIONS_SEP)) {
				if(optionStr == null) {
					continue;
				}
				optionStr = optionStr.trim();

				if(!optionStr.isEmpty()) {
					String[] optionArr = optionStr.split("=");

					String optionName  = optionArr.length > 0 ? optionArr[0].trim() : null;
					String optionValue = optionArr.length > 1 ? optionArr[1].trim() : null;

					if(optionName == null) {
						continue;
					}

					if(optionName.equals(OPTION_IGNORE_CASE)) {
						optIgnoreCase = (optionValue == null || optionValue.isEmpty()) ? true : Boolean.parseBoolean(optionValue);
					} else if(optionName.equals(OPTION_WILD_CARD)) {
						optWildCard = (optionValue == null || optionValue.isEmpty()) ? true : Boolean.parseBoolean(optionValue);
					} else {
						// log warning: unrecognized option..
					}
				}
			}
		}


		if(policyResource != null) {
			policyIsExcludes = policyResource.getIsExcludes();

			if(policyResource.getValues() != null && !policyResource.getValues().isEmpty()) {
				for(String policyValue : policyResource.getValues()) {
					if(policyValue == null) {
						continue;
					}
	
					if(optIgnoreCase) {
						policyValue = policyValue.toLowerCase();
					}
					
					policyValues.add(policyValue);
				}
			}
		}
	}

	@Override
	public boolean isMatch(String value) {
		boolean ret = false;

		if(value != null) {
			if(optIgnoreCase) {
				value = value.toLowerCase();
			}

			for(String policyValue : policyValues) {
				ret = optWildCard ? value.matches(policyValue) : value.equals(policyValue);

				if(ret) {
					break;
				}
			}
		}

		if(policyIsExcludes) {
			ret = !ret;
		}

		return ret;
	}

}
