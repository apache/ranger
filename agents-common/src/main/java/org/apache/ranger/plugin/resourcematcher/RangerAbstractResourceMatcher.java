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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;


public abstract class RangerAbstractResourceMatcher implements RangerResourceMatcher {
	private static final Log LOG = LogFactory.getLog(RangerAbstractResourceMatcher.class);

	public final static String WILDCARD_ASTERISK = "*";

	public final static String OPTIONS_SEP        = ";";
	public final static String OPTION_NV_SEP      = "=";
	public final static String OPTION_IGNORE_CASE = "ignoreCase";
	public final static String OPTION_WILD_CARD   = "wildCard";

	protected RangerResourceDef    resourceDef    = null;
	protected RangerPolicyResource policyResource = null;
	protected Map<String, String>  options        = null;

	protected boolean      optIgnoreCase = false;
	protected boolean      optWildCard   = false;

	protected List<String> policyValues     = null;
	protected boolean      policyIsExcludes = false;
	protected boolean      isMatchAny       = false;

	@Override
	public void setResourceDef(RangerResourceDef resourceDef) {
		this.resourceDef = resourceDef;
		this.options     = resourceDef != null ? resourceDef.getMatcherOptions() : null;
	}

	@Override
	public void setPolicyResource(RangerPolicyResource policyResource) {
		this.policyResource = policyResource;
	}

	@Override
	public void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAbstractResourceMatcher.init()");
		}

		optIgnoreCase = getBooleanOption(OPTION_IGNORE_CASE, true);
		optWildCard   = getBooleanOption(OPTION_WILD_CARD, true);

		policyValues     = new ArrayList<String>();
		policyIsExcludes = policyResource == null ? false : policyResource.getIsExcludes();

		if(policyResource != null && policyResource.getValues() != null) {
			for(String policyValue : policyResource.getValues()) {
				if(StringUtils.isEmpty(policyValue)) {
					continue;
				}

				if(StringUtils.containsOnly(policyValue, WILDCARD_ASTERISK)) {
					isMatchAny = true;
				}

				policyValues.add(policyValue);
			}
		}

		if(policyValues.isEmpty()) {
			isMatchAny = true;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAbstractResourceMatcher.init()");
		}
	}

	@Override
	public boolean isMatchAny() { return isMatchAny; }

	@Override
	public boolean isSingleAndExactMatch(String resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAbstractResourceMatcher.isSingleAndExactMatch(" + resource + ")");
		}

		boolean ret = false;

		if(CollectionUtils.isEmpty(policyValues)) {
			ret = StringUtils.isEmpty(resource);
		} else if(policyValues.size() == 1) {
			String policyValue = policyValues.get(0);
			
			if(isMatchAny) {
				ret = StringUtils.containsOnly(resource, WILDCARD_ASTERISK);
			} else {
				ret = optIgnoreCase ? StringUtils.equalsIgnoreCase(resource, policyValue) : StringUtils.equals(resource, policyValue);
			}

			if(policyIsExcludes) {
				ret = !ret;
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAbstractResourceMatcher.isSingleAndExactMatch(" + resource + "): " + ret);
		}

		return ret;
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

		if(StringUtils.isEmpty(ret)) {
			ret = defaultValue;
		}

		return ret;
	}

	public boolean getBooleanOption(String name) {
		String val = getOption(name);

		boolean ret = StringUtils.isEmpty(val) ? false : Boolean.parseBoolean(val);

		return ret;
	}

	public boolean getBooleanOption(String name, boolean defaultValue) {
		String strVal = getOption(name);

		boolean ret = StringUtils.isEmpty(strVal) ? defaultValue : Boolean.parseBoolean(strVal);

		return ret;
	}

	public char getCharOption(String name, char defaultValue) {
		String strVal = getOption(name);

		char ret = StringUtils.isEmpty(strVal) ? defaultValue : strVal.charAt(0);

		return ret;
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerAbstractResourceMatcher={");

		sb.append("resourceDef={");
		if(resourceDef != null) {
			resourceDef.toString(sb);
		}
		sb.append("} ");
		sb.append("policyResource={");
		if(policyResource != null) {
			policyResource.toString(sb);
		}
		sb.append("} ");
		sb.append("optIgnoreCase={").append(optIgnoreCase).append("} ");
		sb.append("optWildCard={").append(optWildCard).append("} ");

		sb.append("policyValues={");
		if(policyValues != null) {
			for(String value : policyValues) {
				sb.append(value).append(",");
			}
		}
		sb.append("} ");

		sb.append("policyIsExcludes={").append(policyIsExcludes).append("} ");
		sb.append("isMatchAny={").append(isMatchAny).append("} ");

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

	/**
	 * Is resource asking to authorize all possible values at this level?
	 * @param resource
	 * @return
	 */
	boolean isAllValuesRequested(String resource) {
		boolean result = StringUtils.isEmpty(resource) || WILDCARD_ASTERISK.equals(resource);
		if (LOG.isDebugEnabled()) {
			LOG.debug("isAllValuesRequested(" + resource + "): " + result);
		}
		return result;
	}

	/**
	 * The only case where excludes flag does NOT change the result is the following:
	 * - Resource denotes all possible values (i.e. resource in (null, "", "*")
	 * - where as policy does not allow all possible values (i.e. policy.values().contains("*")
	 *
	 * @param allValuesRequested
	 * @param resultWithoutExcludes
     * @return
     */
	public boolean applyExcludes(boolean allValuesRequested, boolean resultWithoutExcludes) {
		if (!policyIsExcludes) return resultWithoutExcludes;                   // not an excludes policy!
		if (allValuesRequested && !isMatchAny)  return resultWithoutExcludes;  // one case where excludes has no effect
		return !resultWithoutExcludes;                                         // all other cases flip it
	}
}
