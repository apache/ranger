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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.util.ServiceDefUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class RangerPathResourceMatcher extends RangerDefaultResourceMatcher {
	private static final Log LOG = LogFactory.getLog(RangerPathResourceMatcher.class);

	public static final String OPTION_PATH_SEPARATOR       = "pathSeparatorChar";
	public static final char   DEFAULT_PATH_SEPARATOR_CHAR = org.apache.hadoop.fs.Path.SEPARATOR_CHAR;

	private boolean policyIsRecursive;
	private char    pathSeparatorChar = '/';

	@Override
	public void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPathResourceMatcher.init()");
		}

		Map<String, String> options = resourceDef == null ? null : resourceDef.getMatcherOptions();

		policyIsRecursive = policyResource != null && policyResource.getIsRecursive();
		pathSeparatorChar = ServiceDefUtil.getCharOption(options, OPTION_PATH_SEPARATOR, DEFAULT_PATH_SEPARATOR_CHAR);

		super.init();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPathResourceMatcher.init()");
		}
	}

	@Override

	protected ResourceMatcherWrapper buildResourceMatchers() {
		List<ResourceMatcher> resourceMatchers = new ArrayList<>();
		boolean needsDynamicEval = false;

		for (String policyValue : policyValues) {
			if (optWildCard && policyIsRecursive) {
				if (policyValue.charAt(policyValue.length() - 1) == pathSeparatorChar) {
					policyValue += WILDCARD_ASTERISK;
				}
			}

			ResourceMatcher matcher = getMatcher(policyValue);

			if (matcher != null) {
				if (matcher.isMatchAny()) {
					resourceMatchers.clear();
					break;
				}
				if (!needsDynamicEval && matcher.getNeedsDynamicEval()) {
					needsDynamicEval = true;
				}
				resourceMatchers.add(matcher);
			}
		}

		Collections.sort(resourceMatchers, new ResourceMatcher.PriorityComparator());

		return CollectionUtils.isNotEmpty(resourceMatchers) ?
				new ResourceMatcherWrapper(needsDynamicEval, resourceMatchers) : null;
	}

	@Override
	ResourceMatcher getMatcher(String policyValue) {
		if(! policyIsRecursive) {
			return super.getMatcher(policyValue);
		}

		final int len = policyValue != null ? policyValue.length() : 0;

		if (len == 0) {
			return null;
		}

		// To ensure that when policyValue is single '*', ResourceMatcher created here returns true for isMatchAny()
		if (optWildCard && WILDCARD_ASTERISK.equals(policyValue)) {
			return new CaseInsensitiveStringMatcher("");
		}

		boolean isWildcardPresent = false;

		if (optWildCard) {
			for (int i = 0; i < len; i++) {
				final char c = policyValue.charAt(i);

				if (c == '?' || c == '*') {
					isWildcardPresent = true;
					break;
				}
			}
		}

		final ResourceMatcher ret;

		if (isWildcardPresent) {
			ret = optIgnoreCase ? new CaseInsensitiveRecursiveWildcardMatcher(policyValue, pathSeparatorChar)
								: new CaseSensitiveRecursiveWildcardMatcher(policyValue, pathSeparatorChar);
		} else {
			ret = optIgnoreCase ? new CaseInsensitiveRecursiveMatcher(policyValue, pathSeparatorChar) : new CaseSensitiveRecursiveMatcher(policyValue, pathSeparatorChar);
		}

		if (optReplaceTokens) {
			ret.setDelimiters(startDelimiterChar, endDelimiterChar, escapeChar, tokenPrefix);
		}

		return ret;
	}

	static boolean isRecursiveWildCardMatch(String pathToCheck, String wildcardPath, char pathSeparatorChar, IOCase caseSensitivity) {

		boolean ret = false;

		if (! StringUtils.isEmpty(pathToCheck)) {
			String[] pathElements = StringUtils.split(pathToCheck, pathSeparatorChar);

			if(! ArrayUtils.isEmpty(pathElements)) {
				StringBuilder sb = new StringBuilder();

				if(pathToCheck.charAt(0) == pathSeparatorChar) {
					sb.append(pathSeparatorChar); // preserve the initial pathSeparatorChar
				}

				for(String p : pathElements) {
					sb.append(p);

					ret = FilenameUtils.wildcardMatch(sb.toString(), wildcardPath, caseSensitivity);

					if (ret) {
						break;
					}

					sb.append(pathSeparatorChar);
				}

				sb = null;
			} else { // pathToCheck consists of only pathSeparatorChar
				ret = FilenameUtils.wildcardMatch(pathToCheck, wildcardPath, caseSensitivity);
			}
		}
		return ret;
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerPathResourceMatcher={");

		super.toString(sb);

		sb.append("policyIsRecursive={").append(policyIsRecursive).append("} ");

		sb.append("}");

		return sb;
	}
}

final class CaseSensitiveRecursiveWildcardMatcher extends ResourceMatcher {
	private final char levelSeparatorChar;
	CaseSensitiveRecursiveWildcardMatcher(String value, char levelSeparatorChar) {
		super(value);
		this.levelSeparatorChar = levelSeparatorChar;
	}

	@Override
	boolean isMatch(String resourceValue, Map<String, Object> evalContext) {
		return RangerPathResourceMatcher.isRecursiveWildCardMatch(resourceValue, getExpandedValue(evalContext), levelSeparatorChar, IOCase.SENSITIVE);
	}
	int getPriority() { return 7 + (getNeedsDynamicEval() ? DYNAMIC_EVALUATION_PENALTY : 0);}
}

final class CaseInsensitiveRecursiveWildcardMatcher extends ResourceMatcher {
	private final char levelSeparatorChar;
	CaseInsensitiveRecursiveWildcardMatcher(String value, char levelSeparatorChar) {
		super(value);
		this.levelSeparatorChar = levelSeparatorChar;
	}

	@Override
	boolean isMatch(String resourceValue, Map<String, Object> evalContext) {
		return RangerPathResourceMatcher.isRecursiveWildCardMatch(resourceValue, getExpandedValue(evalContext), levelSeparatorChar, IOCase.INSENSITIVE);
	}
	int getPriority() { return 8 + (getNeedsDynamicEval() ? DYNAMIC_EVALUATION_PENALTY : 0);}

}

abstract class RecursiveMatcher extends ResourceMatcher {
	final char levelSeparatorChar;
	String valueWithoutSeparator;
	String valueWithSeparator;

	RecursiveMatcher(String value, char levelSeparatorChar) {
		super(value);
		this.levelSeparatorChar = levelSeparatorChar;
	}

	String getStringToCompare(String policyValue) {
		if (StringUtils.isEmpty(policyValue)) {
			return policyValue;
		}
		return (policyValue.lastIndexOf(levelSeparatorChar) == policyValue.length()-1) ?
			policyValue.substring(0, policyValue.length()-1) : policyValue;
	}
}

final class CaseSensitiveRecursiveMatcher extends RecursiveMatcher {
	CaseSensitiveRecursiveMatcher(String value, char levelSeparatorChar) {
		super(value, levelSeparatorChar);
	}

	@Override
	boolean isMatch(String resourceValue, Map<String, Object> evalContext) {

		final String noSeparator;
		if (getNeedsDynamicEval()) {
			String expandedPolicyValue = getExpandedValue(evalContext);
			noSeparator = expandedPolicyValue != null ? getStringToCompare(expandedPolicyValue) : null;
		} else {
			if (valueWithoutSeparator == null && value != null) {
				valueWithoutSeparator = getStringToCompare(value);
				valueWithSeparator = valueWithoutSeparator + Character.toString(levelSeparatorChar);
			}
			noSeparator = valueWithoutSeparator;
		}

		boolean ret = StringUtils.equals(resourceValue, noSeparator);

		if (!ret && noSeparator != null) {
			final String withSeparator = getNeedsDynamicEval() ? noSeparator + Character.toString(levelSeparatorChar) : valueWithSeparator;
			ret = StringUtils.startsWith(resourceValue, withSeparator);
		}

		return ret;
	}
	int getPriority() { return 7 + (getNeedsDynamicEval() ? DYNAMIC_EVALUATION_PENALTY : 0);}
}

final class CaseInsensitiveRecursiveMatcher extends RecursiveMatcher {
	CaseInsensitiveRecursiveMatcher(String value, char levelSeparatorChar) {
		super(value, levelSeparatorChar);
	}

	@Override
	boolean isMatch(String resourceValue, Map<String, Object> evalContext) {

		final String noSeparator;
		if (getNeedsDynamicEval()) {
			String expandedPolicyValue = getExpandedValue(evalContext);
			noSeparator = expandedPolicyValue != null ? getStringToCompare(expandedPolicyValue) : null;
		} else {
			if (valueWithoutSeparator == null && value != null) {
				valueWithoutSeparator = getStringToCompare(value);
				valueWithSeparator = valueWithoutSeparator + Character.toString(levelSeparatorChar);
			}
			noSeparator = valueWithoutSeparator;
		}

		boolean ret = StringUtils.equalsIgnoreCase(resourceValue, noSeparator);

		if (!ret && noSeparator != null) {
			final String withSeparator = getNeedsDynamicEval() ? noSeparator + Character.toString(levelSeparatorChar) : valueWithSeparator;
			ret = StringUtils.startsWithIgnoreCase(resourceValue, withSeparator);
		}

		return ret;
	}

	int getPriority() { return 8 + (getNeedsDynamicEval() ? DYNAMIC_EVALUATION_PENALTY : 0);}

}