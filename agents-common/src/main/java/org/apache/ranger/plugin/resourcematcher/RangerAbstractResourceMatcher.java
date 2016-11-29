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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;


public abstract class RangerAbstractResourceMatcher implements RangerResourceMatcher {
	private static final Log LOG = LogFactory.getLog(RangerAbstractResourceMatcher.class);

	public final static String WILDCARD_ASTERISK = "*";
	public final static String WILDCARDS = "*?";

	public final static String OPTIONS_SEP        = ";";
	public final static String OPTION_NV_SEP      = "=";
	public final static String OPTION_IGNORE_CASE = "ignoreCase";
	public final static String OPTION_WILD_CARD   = "wildCard";

	protected RangerResourceDef    resourceDef    = null;
	protected RangerPolicyResource policyResource = null;

	protected boolean      optIgnoreCase = false;
	protected boolean      optWildCard   = false;

	protected List<String> policyValues     = null;
	protected boolean      policyIsExcludes = false;
	protected boolean      isMatchAny       = false;
	protected List<ResourceMatcher> resourceMatchers = null;

	@Override
	public void setResourceDef(RangerResourceDef resourceDef) {
		this.resourceDef = resourceDef;
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
			for (String policyValue : policyResource.getValues()) {
				if (StringUtils.isEmpty(policyValue)) {
					continue;
				}
				policyValues.add(policyValue);
			}
		}
		resourceMatchers = buildResourceMatchers();
		isMatchAny = CollectionUtils.isEmpty(resourceMatchers);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAbstractResourceMatcher.init()");
		}
	}

	@Override
	public boolean isMatchAny() { return isMatchAny; }

	protected List<ResourceMatcher> buildResourceMatchers() {
		List<ResourceMatcher> ret = new ArrayList<ResourceMatcher> ();

		for (String policyValue : policyValues) {
			ResourceMatcher matcher = getMatcher(policyValue);

			if (matcher != null) {
				if (matcher.isMatchAny()) {
					ret.clear();
					break;
				} else {
					ret.add(matcher);
				}
			}
		}

		Collections.sort(ret);

		return ret;
	}

	@Override
	public boolean isCompleteMatch(String resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAbstractResourceMatcher.isCompleteMatch(" + resource + ")");
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
			LOG.debug("<== RangerAbstractResourceMatcher.isCompleteMatch(" + resource + "): " + ret);
		}

		return ret;
	}


	public String getOption(String name) {
		String ret = null;

		Map<String, String> options = resourceDef != null ? resourceDef.getMatcherOptions() : null;

		if(options != null && name != null) {
			ret = options.get(name);
		}

		return ret;
	}

	public String getOption(String name, String defaultValue) {
		String ret = defaultValue;
		String val = getOption(name);

		if(val != null) {
			ret = val;
		}

		return ret;
	}

	public boolean getBooleanOption(String name, boolean defaultValue) {
		boolean ret = defaultValue;
		String  val = getOption(name);

		if(val != null) {
			ret = Boolean.parseBoolean(val);
		}

		return ret;
	}

	public char getCharOption(String name, char defaultValue) {
		char   ret = defaultValue;
		String val = getOption(name);

		if(! StringUtils.isEmpty(val)) {
			ret = val.charAt(0);
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
		if(resourceDef != null && resourceDef.getMatcherOptions() != null) {
			for(Map.Entry<String, String> e : resourceDef.getMatcherOptions().entrySet()) {
				sb.append(e.getKey()).append("=").append(e.getValue()).append(OPTIONS_SEP);
			}
		}
		sb.append("} ");

		sb.append("}");

		return sb;
	}

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
     */
	public boolean applyExcludes(boolean allValuesRequested, boolean resultWithoutExcludes) {
		if (!policyIsExcludes) return resultWithoutExcludes;                   // not an excludes policy!
		if (allValuesRequested && !isMatchAny)  return resultWithoutExcludes;  // one case where excludes has no effect
		return !resultWithoutExcludes;                                         // all other cases flip it
	}

	ResourceMatcher getMatcher(String policyValue) {
		final int len = policyValue != null ? policyValue.length() : 0;

		if (len == 0) {
			return null;
		}

		final ResourceMatcher ret;

		int wildcardStartIdx = -1;
		int wildcardEndIdx = -1;
		boolean needWildcardMatch = false;

		// If optWildcard is true
		//   If ('?' found or non-contiguous '*'s found in policyValue)
		//	   needWildcardMatch = true
		// 	 End
		//
		// 	 wildcardStartIdx is set to index of first '*' in policyValue or -1 if '*' is not found in policyValue, and
		// 	 wildcardEndIdx is set to index of last '*' in policyValue or -1 if '*' is not found in policyValue
		// Else
		// 	 needWildcardMatch is set to false
		// End
		if (optWildCard) {
			for (int i = 0; i < len; i++) {
				final char c = policyValue.charAt(i);

				if (c == '?') {
					needWildcardMatch = true;
					break;
				} else if (c == '*') {
					if (wildcardEndIdx == -1 || wildcardEndIdx == (i - 1)) {
						wildcardEndIdx = i;
						if (wildcardStartIdx == -1) {
							wildcardStartIdx = i;
						}
					} else {
						needWildcardMatch = true;
						break;
					}
				}
			}
		}

		if (needWildcardMatch) {
			ret = optIgnoreCase ? new CaseInsensitiveWildcardMatcher(policyValue) : new CaseSensitiveWildcardMatcher(policyValue);
		} else if (wildcardStartIdx == -1) {
			ret = optIgnoreCase ? new CaseInsensitiveStringMatcher(policyValue) : new CaseSensitiveStringMatcher(policyValue);
		} else if (wildcardStartIdx == 0) {
			String matchStr = policyValue.substring(wildcardEndIdx + 1);
			ret = optIgnoreCase ? new CaseInsensitiveEndsWithMatcher(matchStr) : new CaseSensitiveEndsWithMatcher(matchStr);
		} else {
			String matchStr = policyValue.substring(0, wildcardStartIdx);
			ret = optIgnoreCase ? new CaseInsensitiveStartsWithMatcher(matchStr) : new CaseSensitiveStartsWithMatcher(matchStr);
		}

		return ret;
	}
}

final class CaseSensitiveStringMatcher extends ResourceMatcher {
	CaseSensitiveStringMatcher(String value) {
		super(value);
	}

	boolean isMatch(String str) {
		return StringUtils.equals(str, value);
	}
	int getPriority() { return 1;}
}

final class CaseInsensitiveStringMatcher extends ResourceMatcher {
	CaseInsensitiveStringMatcher(String value) { super(value); }

	boolean isMatch(String str) {
		return StringUtils.equalsIgnoreCase(str, value);
	}
	int getPriority() {return 2; }
}

final class CaseSensitiveStartsWithMatcher extends ResourceMatcher {
	CaseSensitiveStartsWithMatcher(String value) {
		super(value);
	}

	boolean isMatch(String str) {
		return StringUtils.startsWith(str, value);
	}
	int getPriority() { return 3;}
}

final class CaseInsensitiveStartsWithMatcher extends ResourceMatcher {
	CaseInsensitiveStartsWithMatcher(String value) { super(value); }

	boolean isMatch(String str) {
		return StringUtils.startsWithIgnoreCase(str, value);
	}
	int getPriority() { return 4; }
}

final class CaseSensitiveEndsWithMatcher extends ResourceMatcher {
	CaseSensitiveEndsWithMatcher(String value) {
		super(value);
	}

	boolean isMatch(String str) {
		return StringUtils.endsWith(str, value);
	}
	int getPriority() { return 3; }
}

final class CaseInsensitiveEndsWithMatcher extends ResourceMatcher {
	CaseInsensitiveEndsWithMatcher(String value) {
		super(value);
	}

	boolean isMatch(String str) {
		return StringUtils.endsWithIgnoreCase(str, value);
	}
	int getPriority() { return 4; }
}

final class CaseSensitiveWildcardMatcher extends ResourceMatcher {
	CaseSensitiveWildcardMatcher(String value) {
		super(value);
	}

	boolean isMatch(String str) {
		return FilenameUtils.wildcardMatch(str, value, IOCase.SENSITIVE);
	}
	int getPriority() { return 5; }
}


final class CaseInsensitiveWildcardMatcher extends ResourceMatcher {
	CaseInsensitiveWildcardMatcher(String value) {
		super(value);
	}

	boolean isMatch(String str) {
		return FilenameUtils.wildcardMatch(str, value, IOCase.INSENSITIVE);
	}
	int getPriority() {return 6; }

}

