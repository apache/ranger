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
import java.util.HashMap;
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

	public final String WILDCARD_PATTERN = ".*";

	public final String OPTIONS_SEP        = ";";
	public final String OPTION_NV_SEP      = "=";
	public final String OPTION_IGNORE_CASE = "ignoreCase";
	public final String OPTION_WILD_CARD   = "wildCard";

	private RangerResourceDef    resourceDef    = null;
	private RangerPolicyResource policyResource = null;
	private String               optionsString  = null;
	private Map<String, String>  options        = null;

	protected boolean      optIgnoreCase = false;
	protected boolean      optWildCard   = false;

	protected List<String> policyValues     = null;
	protected boolean      policyIsExcludes = false;
	protected boolean      isMatchAny       = false;

	@Override
	public void init(RangerResourceDef resourceDef, RangerPolicyResource policyResource, String optionsString) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAbstractResourceMatcher.init(" + resourceDef + ", " + policyResource + ", " + optionsString + ")");
		}

		this.resourceDef    = resourceDef;
		this.policyResource = policyResource;
		this.optionsString  = optionsString;

		options = new HashMap<String, String>();

		if(optionsString != null) {
			for(String optionString : optionsString.split(OPTIONS_SEP)) {
				if(StringUtils.isEmpty(optionString)) {
					continue;
				}

				String[] nvArr = optionString.split(OPTION_NV_SEP);

				String name  = (nvArr != null && nvArr.length > 0) ? nvArr[0].trim() : null;
				String value = (nvArr != null && nvArr.length > 1) ? nvArr[1].trim() : null;

				if(StringUtils.isEmpty(name)) {
					continue;
				}

				options.put(name, value);
			}
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

				if(optIgnoreCase) {
					policyValue = policyValue.toLowerCase();
				}

				if(optWildCard) {
					policyValue = getWildCardPattern(policyValue);
				}

				if(policyValue.equals(WILDCARD_PATTERN)) {
					isMatchAny = true;
				}

				policyValues.add(policyValue);
			}
		}

		if(policyValues.isEmpty()) {
			isMatchAny = true;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAbstractResourceMatcher.init(" + resourceDef + ", " + policyResource + ", " + optionsString + ")");
		}
	}

	@Override
	public RangerResourceDef getResourceDef() {
		return resourceDef;
	}

	@Override
	public RangerPolicyResource getPolicyResource() {
		return policyResource;
	}

	@Override
	public String getOptionsString() {
		return optionsString;
	}

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
				ret = StringUtils.equals(resource, "*");
			} else {
				ret = optIgnoreCase ? StringUtils.equalsIgnoreCase(resource, policyValue) : StringUtils.equals(resource, policyValue);
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

	public static String getWildCardPattern(String policyValue) {
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
		sb.append("optionsString={").append(optionsString).append("} ");
		sb.append("optIgnoreCase={").append(optIgnoreCase).append("} ");
		sb.append("optWildCard={").append(optWildCard).append("} ");
		sb.append("policyValues={").append(StringUtils.join(policyValues, ",")).append("} ");
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
}
