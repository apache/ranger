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

package org.apache.ranger.plugin.resourcematcher;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;


public abstract class RangerAbstractResourceMatcher implements RangerResourceMatcher {
	private static final Log LOG = LogFactory.getLog(RangerAbstractResourceMatcher.class);

	public final String OPTIONS_SEP        = ";";
	public final String OPTION_NV_SEP      = "=";
	public final String OPTION_IGNORE_CASE = "ignoreCase";
	public final String OPTION_WILD_CARD   = "wildCard";

	private RangerPolicyResource policyResource = null;
	private String               optionsString  = null;
	private Map<String, String>  options        = null;

	protected boolean optIgnoreCase    = false;
	protected boolean optWildCard      = false;

	@Override
	public void init(RangerPolicyResource policyResource, String optionsString) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAbstractResourceMatcher.init(" + policyResource + ", " + optionsString + ")");
		}

		this.policyResource = policyResource;
		this.optionsString  = optionsString;

		options = new HashMap<String, String>();

		if(optionsString != null) {
			for(String optionString : optionsString.split(OPTIONS_SEP)) {
				if(optionString == null) {
					continue;
				}
				optionString = optionString.trim();

				if(!optionString.isEmpty()) {
					String[] nvArr = optionString.split(OPTION_NV_SEP);

					String name  = (nvArr != null && nvArr.length > 0) ? nvArr[0].trim() : null;
					String value = (nvArr != null && nvArr.length > 1) ? nvArr[1].trim() : null;

					if(name == null || name.isEmpty()) {
						continue;
					}

					options.put(name, value);
				}
			}
		}

		optIgnoreCase = getBooleanOption(OPTION_IGNORE_CASE, true);
		optWildCard   = getBooleanOption(OPTION_WILD_CARD, true);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAbstractResourceMatcher.init(" + policyResource + ", " + optionsString + ")");
		}
	}

	@Override
	public RangerPolicyResource getPolicyResource() {
		return policyResource;
	}

	@Override
	public String getOptionsString() {
		return optionsString;
	}


	public String getOption(String name) {
		String ret = null;

		if(options != null && name != null) {
			ret = options.get(name);
		}

		return ret;
	}

	public String getOption(String name, String defaultValue) {
		String ret = getOption(name);

		if(ret == null) {
			ret = defaultValue;
		}

		return ret;
	}

	public boolean getBooleanOption(String name) {
		String val = getOption(name);

		boolean ret = (val == null) ? false : Boolean.parseBoolean(val);

		return ret;
	}

	public boolean getBooleanOption(String name, boolean defaultValue) {
		String strVal = getOption(name);

		boolean ret = (strVal == null) ? defaultValue : Boolean.parseBoolean(strVal);

		return ret;
	}

	public String getWildCardPattern(String policyValue) {
		if (policyValue != null) {
			policyValue = policyValue.replaceAll("\\?", "\\.") 
									 .replaceAll("\\*", ".*") ;
		}

		return policyValue ;
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerAbstractResourceMatcher={");

		sb.append("policyResource={");
		if(policyResource != null) {
			policyResource.toString(sb);
		}
		sb.append("} ");
		sb.append("optionsString={").append(optionsString).append("} ");
		sb.append("optIgnoreCase={").append(optIgnoreCase).append("} ");
		sb.append("optWildCard={").append(optWildCard).append("} ");

		sb.append("options={");
		if(options != null) {
			for(Map.Entry<String, String> e : options.entrySet()) {
				sb.append(e.getKey()).append("=").append(e.getValue()).append(OPTIONS_SEP);
			}
		}
		sb.append("} ");

		sb.append("}");

		return sb;
	}
}
