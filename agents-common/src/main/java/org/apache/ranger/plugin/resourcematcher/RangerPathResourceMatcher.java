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
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;


public class RangerPathResourceMatcher extends RangerDefaultResourceMatcher {
	private static final Logger LOG = LoggerFactory.getLogger(RangerPathResourceMatcher.class);

	public static final String OPTION_PATH_SEPARATOR       = "pathSeparatorChar";
	public static final char   DEFAULT_PATH_SEPARATOR_CHAR = org.apache.hadoop.fs.Path.SEPARATOR_CHAR;

	private boolean   policyIsRecursive;
	private Character pathSeparatorChar = '/';

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
		if (!policyIsRecursive) {
			return getPathMatcher(policyValue);
		}

		final int len = policyValue != null ? policyValue.length() : 0;

		if (len == 0) {
			return null;
		}

		// To ensure that when policyValue is single '*', ResourceMatcher created here returns true for isMatchAny()
		if (optWildCard && WILDCARD_ASTERISK.equals(policyValue)) {
			return new CaseInsensitiveStringMatcher("", getOptions());
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
			ret = new RecursiveWildcardResourceMatcher(policyValue, getOptions(), pathSeparatorChar, optIgnoreCase, RangerPathResourceMatcher::isRecursiveWildCardMatch, optIgnoreCase ? 8 : 7);
		} else {
			ret = new RecursivePathResourceMatcher(policyValue, getOptions(), pathSeparatorChar, optIgnoreCase, optIgnoreCase ? 8 : 7);
		}

		if (optReplaceTokens) {
			ret.setDelimiters(startDelimiterChar, endDelimiterChar, escapeChar, tokenPrefix);
		}

		return ret;
	}

	static boolean isRecursiveWildCardMatch(String pathToCheck, String wildcardPath, Character pathSeparatorChar, IOCase caseSensitivity, String[] wildcardPathElements) {

		boolean ret = false;

		if (! StringUtils.isEmpty(pathToCheck)) {
			String[] pathElements = StringUtils.split(pathToCheck, pathSeparatorChar);

			if(! ArrayUtils.isEmpty(pathElements)) {
				StringBuilder sb = new StringBuilder();

				if(pathToCheck.charAt(0) == pathSeparatorChar) {
					sb.append(pathSeparatorChar); // preserve the initial pathSeparatorChar
				}

				int      pathElementIndex     = 0;
				boolean  useStringMatching    = true;

				for (String p : pathElements) {
					sb.append(p);

					if (useStringMatching) {
						if (wildcardPathElements.length > pathElementIndex) {
							String wp = wildcardPathElements[pathElementIndex];

							if (!(StringUtils.contains(wp, '*') || StringUtils.contains(wp, '?'))) {
								boolean isMatch = caseSensitivity.isCaseSensitive() ? StringUtils.equals(p, wp) : StringUtils.equalsIgnoreCase(p, wp);
								if (!isMatch) {
									useStringMatching = false;
									break;
								}
							} else {
								useStringMatching = false;
							}
						} else {
							useStringMatching = false;
						}
					}

					if (!useStringMatching) {
						ret = FilenameUtils.wildcardMatch(sb.toString(), wildcardPath, caseSensitivity);
						if (ret) {
							break;
						}
					}

					sb.append(pathSeparatorChar);
					pathElementIndex++;
				}
				if (useStringMatching) {
					if (pathElements.length == wildcardPathElements.length) { // Loop finished normally and all sub-paths string-matched..
						ret = true;
					} else if (pathToCheck.charAt(pathToCheck.length() - 1) == pathSeparatorChar) { // pathToCheck ends with separator, like /home/
						ret = pathElements.length == (wildcardPathElements.length - 1) && WILDCARD_ASTERISK.equals(wildcardPathElements[wildcardPathElements.length - 1]);
					}
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

	private ResourceMatcher getPathMatcher(String policyValue) {
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

		if (needWildcardMatch) { // test?, test*a*, test*a*b, *test*a
			ret = new WildcardResourceMatcher(policyValue, getOptions(), pathSeparatorChar, optIgnoreCase, FilenameUtils::wildcardMatch, 6);
		} else if (wildcardStartIdx == -1) { // test, testa, testab
			ret = new PathResourceMatcher(policyValue, getOptions(), pathSeparatorChar, optIgnoreCase ? StringUtils::equalsIgnoreCase : StringUtils::equals, !optIgnoreCase, optIgnoreCase ? 2 : 1);
		} else if (wildcardStartIdx == 0) { // *test, **test, *testa, *testab
			String matchStr = policyValue.substring(wildcardEndIdx + 1);
			ret = new PathEndsWithResourceMatcher(matchStr, getOptions(), pathSeparatorChar, !optIgnoreCase, optIgnoreCase ? 4 : 3);
		} else if (wildcardEndIdx != (len - 1)) { // test*a, test*ab
			ret = new WildcardResourceMatcher(policyValue, getOptions(), pathSeparatorChar, optIgnoreCase, FilenameUtils::wildcardMatch, 6);
		} else { // test*, test**, testa*, testab*
			String matchStr = policyValue.substring(0, wildcardStartIdx);
			ret = new PathStartsWithResourceMatcher(matchStr, getOptions(), pathSeparatorChar, !optIgnoreCase, optIgnoreCase ? 4 : 3);
		}

		if (optReplaceTokens) {
			ret.setDelimiters(startDelimiterChar, endDelimiterChar, escapeChar, tokenPrefix);
		}

		return ret;
	}

	interface TriFunction<T, U, V, R> {
		R apply(T t, U u, V v);
	}

	interface QuadFunction<T, U, V, W, R> {
		R apply(T t, U u, V v, W w);
	}

	interface QuintFunction<T, U, V, W, R, X> {
		R apply(T t, U u, V v, W w, X x);
	}

	static abstract class AbstractPathResourceMatcher extends ResourceMatcher {
		final char    pathSeparatorChar;
		final int     priority;
		final boolean isCaseSensitive;

		AbstractPathResourceMatcher(String value, Map<String, String> options, char pathSeparatorChar, boolean isCaseSensitive, int priority) {
			super(value, options);

			this.pathSeparatorChar = pathSeparatorChar;
			this.priority          = priority;
			this.isCaseSensitive   = isCaseSensitive;
		}
		int getPriority() {
			return priority + (getNeedsDynamicEval() ? DYNAMIC_EVALUATION_PENALTY : 0);
		}
	}

	static class PathResourceMatcher extends AbstractPathResourceMatcher {
		final BiFunction<String, String, Boolean> function;

		PathResourceMatcher(String value, Map<String, String> options, char pathSeparatorChar, BiFunction<String, String, Boolean> function, boolean isCaseSensitive, int priority) {
			super(value, options, pathSeparatorChar, isCaseSensitive, priority);

			this.function = function;
		}

		@Override
		boolean isMatch(String resourceValue, Map<String, Object> evalContext) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> PathResourceMatcher.isMatch(resourceValue={}, evalContext={})", resourceValue, evalContext);
			}

			String  expandedValue = getExpandedValue(evalContext);
			boolean ret           = function.apply(resourceValue, expandedValue);

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== PathResourceMatcher.isMatch(resourceValue={}, expandedValue={}): ret={}", resourceValue, getExpandedValue(evalContext) , ret );
			}

			return ret;
		}

		@Override
		public boolean isPrefixMatch(String resourceValue, Map<String, Object> evalContext) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> PathResourceMatcher.isPrefixMatch(resourceValue={}, evalContext={})", resourceValue, evalContext);
			}

			boolean ret = isCaseSensitive ? StringUtils.startsWith(getExpandedValue(evalContext), resourceValue)
			                              : StringUtils.startsWithIgnoreCase(getExpandedValue(evalContext), resourceValue);

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== PathResourceMatcher.isPrefixMatch(resourceValue={}, expandedValue={}): ret={}", resourceValue, getExpandedValue(evalContext) , ret );
			}

			return ret;

		}

		@Override
		public boolean isChildMatch(String resourceValue, Map<String, Object> evalContext) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> PathResourceMatcher.isChildMatch(resourceValue={}, evalContext={})", resourceValue, evalContext);
			}

			boolean ret                     = false;
			String  expandedValue           = getExpandedValue(evalContext);
			int     lastLevelSeparatorIndex = expandedValue.lastIndexOf(pathSeparatorChar);

			if (lastLevelSeparatorIndex != -1) {
				String shorterExpandedValue = expandedValue.substring(0, lastLevelSeparatorIndex);

				if (resourceValue.charAt(resourceValue.length()-1) == pathSeparatorChar) {
					resourceValue = resourceValue.substring(0, resourceValue.length()-1);
				}

				ret = function.apply(resourceValue, shorterExpandedValue);
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== PathResourceMatcher.isChildMatch(resourceValue={}, lastLevelSeparatorIndex={}): ret={}", resourceValue, lastLevelSeparatorIndex, ret );
			}

			return ret;
		}
	}

	static class PathStartsWithResourceMatcher extends AbstractPathResourceMatcher {
		PathStartsWithResourceMatcher(String value, Map<String, String> options, char pathSeparatorChar, boolean isCaseSensitive, int priority) {
			super(value, options, pathSeparatorChar, isCaseSensitive, priority);
		}

		@Override
		boolean isMatch(String resourceValue, Map<String, Object> evalContext) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> PathStartsWithResourceMatcher.isMatch(resourceValue={}, evalContext={})", resourceValue, evalContext);
			}

			boolean ret = isCaseSensitive ? StringUtils.startsWith(resourceValue, getExpandedValue(evalContext))
			                              : StringUtils.startsWithIgnoreCase(resourceValue, getExpandedValue(evalContext));

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== PathStartsWithResourceMatcher.isMatch(resourceValue={}, expandedValue={}): ret={}", resourceValue, getExpandedValue(evalContext) , ret );
			}

			return ret;
		}

		@Override
		public boolean isPrefixMatch(String resourceValue, Map<String, Object> evalContext) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> PathStartsWithResourceMatcher.isPrefixMatch(resourceValue={}, evalContext={})", resourceValue, evalContext);
			}

			boolean ret = isCaseSensitive ? StringUtils.startsWith(getExpandedValue(evalContext), resourceValue)
			                              : StringUtils.startsWithIgnoreCase(getExpandedValue(evalContext), resourceValue);

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== PathStartsWithResourceMatcher.isPrefixMatch(resourceValue={}, expandedValue={}): ret={}", resourceValue, getExpandedValue(evalContext) , ret );
			}

			return ret;
		}

		@Override
		public boolean isChildMatch(String resourceValue, Map<String, Object> evalContext) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> PathStartsWithResourceMatcher.isChildMatch(resourceValue={}, evalContext={})", resourceValue, evalContext);
			}

			boolean ret                     = false;
			String  expandedValue           = getExpandedValue(evalContext);
			int     lastLevelSeparatorIndex = expandedValue.lastIndexOf(pathSeparatorChar);

			if (lastLevelSeparatorIndex != -1) {
				String shorterExpandedValue = expandedValue.substring(0, lastLevelSeparatorIndex);

				if (resourceValue.charAt(resourceValue.length()-1) == pathSeparatorChar) {
					resourceValue = resourceValue.substring(0, resourceValue.length()-1);
				}

				ret = isCaseSensitive ? StringUtils.startsWith(resourceValue, shorterExpandedValue)
				                      : StringUtils.startsWithIgnoreCase(resourceValue, shorterExpandedValue);
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== PathStartsWithResourceMatcher.isChildMatch(resourceValue={}, lastLevelSeparatorIndex={}): ret={}", resourceValue, lastLevelSeparatorIndex, ret );
			}

			return ret;
		}
	}

	static class PathEndsWithResourceMatcher extends AbstractPathResourceMatcher {
		PathEndsWithResourceMatcher(String value, Map<String, String> options, char pathSeparatorChar, boolean isCaseSensitive, int priority) {
			super(value, options, pathSeparatorChar, isCaseSensitive, priority);
		}

		@Override
		boolean isMatch(String resourceValue, Map<String, Object> evalContext) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> PathEndsWithResourceMatcher.isMatch(resourceValue={}, evalContext={})", resourceValue, evalContext);
			}

			boolean ret = isCaseSensitive ? StringUtils.endsWith(resourceValue, getExpandedValue(evalContext))
			                              : StringUtils.endsWithIgnoreCase(resourceValue, getExpandedValue(evalContext));

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== PathEndsWithResourceMatcher.isMatch(resourceValue={}, expandedValue={}): ret={}", resourceValue, getExpandedValue(evalContext) , ret );
			}

			return ret;
		}

		@Override
		public boolean isPrefixMatch(String resourceValue, Map<String, Object> evalContext) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> PathEndsWithResourceMatcher.isPrefixMatch(resourceValue={}, evalContext={})", resourceValue, evalContext);
			}

			boolean ret = true; // isPrefixMatch() is always true for endsWith

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== PathEndsWithResourceMatcher.isPrefixMatch(resourceValue={}, expandedValue={}): ret={}", resourceValue, getExpandedValue(evalContext) , ret );
			}

			return ret;
		}

		@Override
		public boolean isChildMatch(String resourceValue, Map<String, Object> evalContext) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> PathEndsWithResourceMatcher.isChildMatch(resourceValue={}, evalContext={})", resourceValue, evalContext);
			}

			boolean ret                     = false;
			String  expandedValue           = getExpandedValue(evalContext);
			int     lastLevelSeparatorIndex = expandedValue.lastIndexOf(pathSeparatorChar);

			if (lastLevelSeparatorIndex != -1) {
				String shorterExpandedValue = expandedValue.substring(0, lastLevelSeparatorIndex);

				if (resourceValue.charAt(resourceValue.length()-1) == pathSeparatorChar) {
					resourceValue = resourceValue.substring(0, resourceValue.length()-1);
				}

				ret = isCaseSensitive ? StringUtils.endsWith(resourceValue, shorterExpandedValue)
				                      : StringUtils.endsWithIgnoreCase(resourceValue, shorterExpandedValue);
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== PathEndsWithResourceMatcher.isChildMatch(resourceValue={}, lastLevelSeparatorIndex={}): ret={}", resourceValue, lastLevelSeparatorIndex, ret );
			}

			return ret;
		}
	}

	static class WildcardResourceMatcher extends AbstractPathResourceMatcher {
		final TriFunction<String, String, IOCase, Boolean> function;
		final IOCase                                       ioCase;

		WildcardResourceMatcher(String value, Map<String, String> options, char pathSeparatorChar, boolean optIgnoreCase, TriFunction<String, String, IOCase, Boolean> function, int priority) {
			super(value, options, pathSeparatorChar, !optIgnoreCase, priority);

			this.function = function;
			this.ioCase   = optIgnoreCase ? IOCase.INSENSITIVE : IOCase.SENSITIVE;
		}

		@Override
		public boolean isMatch(String resourceValue, Map<String, Object> evalContext) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> WildcardResourceMatcher.isMatch(resourceValue=" + resourceValue + ", evalContext=" + evalContext + ")");
			}

			String  expandedValue = getExpandedValue(evalContext);
			boolean ret           = function.apply(resourceValue, expandedValue, ioCase);

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== WildcardResourceMatcher.isMatch(resourceValue=" + resourceValue + ", expandedValue=" + expandedValue + ") : result:[" + ret + "]");
			}
			return ret;
		}

		@Override
		public boolean isPrefixMatch(String resourceValue, Map<String, Object> evalContext) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> WildcardResourceMatcher.isPrefixMatch(resourceValue=" + resourceValue + ", evalContext=" + evalContext + ")");
			}

			boolean ret = ResourceMatcher.wildcardPrefixMatch(resourceValue, getExpandedValue(evalContext), ioCase);

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== WildcardResourceMatcher.isPrefixMatch(resourceValue=" + resourceValue + ", expandedValue=" + getExpandedValue(evalContext) + ") : result:[" + ret + "]");
			}
			return ret;
		}

		@Override
		public boolean isChildMatch(String resourceValue, Map<String, Object> evalContext) {
			boolean ret                     = false;
			String  expandedValue           = getExpandedValue(evalContext);
			int     lastLevelSeparatorIndex = expandedValue.lastIndexOf(pathSeparatorChar);

			if (lastLevelSeparatorIndex != -1) {
				String shorterExpandedValue = expandedValue.substring(0, lastLevelSeparatorIndex);

				if (resourceValue.charAt(resourceValue.length()-1) == pathSeparatorChar) {
					resourceValue = resourceValue.substring(0, resourceValue.length()-1);
				}

				ret = function.apply(resourceValue, shorterExpandedValue, ioCase);
			}

			return ret;
		}
	}

	static class RecursiveWildcardResourceMatcher extends AbstractPathResourceMatcher {
		final QuintFunction<String, String, Character, IOCase, Boolean, String[]> function;
		final IOCase ioCase;
		String[] wildcardPathElements;

		RecursiveWildcardResourceMatcher(String value, Map<String, String> options, char pathSeparatorChar, boolean optIgnoreCase, QuintFunction<String, String, Character, IOCase, Boolean, String[]> function, int priority) {
			super(value, options, pathSeparatorChar, !optIgnoreCase, priority);

			this.function = function;
			this.ioCase   = optIgnoreCase ? IOCase.INSENSITIVE : IOCase.SENSITIVE;

			if (!getNeedsDynamicEval()) {
				wildcardPathElements = StringUtils.split(value, pathSeparatorChar);
			}
		}
		@Override
		boolean isMatch(String resourceValue, Map<String, Object> evalContext) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> RecursiveWildcardResourceMatcher.isMatch(resourceValue=" + resourceValue + ", evalContext=" + evalContext + ")");
			}
			String expandedValue;
			if (getNeedsDynamicEval()) {
				expandedValue = getExpandedValue(evalContext);
				wildcardPathElements = StringUtils.split(expandedValue, pathSeparatorChar);
			} else {
				expandedValue = value;
			}

			boolean ret = function.apply(resourceValue, expandedValue, pathSeparatorChar, ioCase, wildcardPathElements);

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== RecursiveWildcardResourceMatcher.isMatch(resourceValue=" + resourceValue + ", expandedValue=" + expandedValue + ") : result:[" + ret + "]");
			}
			return ret;
		}

		@Override
		public boolean isPrefixMatch(String resourceValue, Map<String, Object> evalContext) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> RecursiveWildcardResourceMatcher.isPrefixMatch(resourceValue=" + resourceValue + ", evalContext=" + evalContext + ")");
			}

			boolean ret = ResourceMatcher.wildcardPrefixMatch(resourceValue, getExpandedValue(evalContext), ioCase);

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== RecursiveWildcardResourceMatcher.isPrefixMatch(resourceValue=" + resourceValue + ", expandedValue=" + getExpandedValue(evalContext) + ") : result:[" + ret + "]");
			}
			return ret;
		}

		@Override
		public boolean isChildMatch(String resourceValue, Map<String, Object> evalContext) {
			boolean ret = false;
			String expandedValue = getExpandedValue(evalContext);
			int lastLevelSeparatorIndex = expandedValue.lastIndexOf(pathSeparatorChar);

			if (lastLevelSeparatorIndex != -1) {
				String shorterExpandedValue = expandedValue.substring(0, lastLevelSeparatorIndex);

				if (resourceValue.charAt(resourceValue.length() - 1) == pathSeparatorChar) {
					resourceValue = resourceValue.substring(0, resourceValue.length() - 1);
				}

				String[] shorterWildCardPathElements = StringUtils.split(shorterExpandedValue, pathSeparatorChar);

				ret = function.apply(resourceValue, shorterExpandedValue, pathSeparatorChar, ioCase, shorterWildCardPathElements);
			}

			return ret;
		}
	}

	static class RecursivePathResourceMatcher extends AbstractPathResourceMatcher {
		String valueWithoutSeparator;
		String valueWithSeparator;

		final IOCase                              ioCase;
		final BiFunction<String, String, Boolean> primaryFunction;
		final BiFunction<String, String, Boolean> fallbackFunction;

		RecursivePathResourceMatcher(String value, Map<String, String> options, char pathSeparatorChar, boolean optIgnoreCase, int priority) {
			super(value, options, pathSeparatorChar, true, priority);

			this.ioCase           = optIgnoreCase ? IOCase.INSENSITIVE : IOCase.SENSITIVE;
			this.primaryFunction  = optIgnoreCase ? StringUtils::equalsIgnoreCase : StringUtils::equals;
			this.fallbackFunction = optIgnoreCase ? StringUtils::startsWithIgnoreCase : StringUtils::startsWith;
		}

		String getStringToCompare(String policyValue) {
			if (StringUtils.isEmpty(policyValue)) {
				return policyValue;
			}
			return (policyValue.lastIndexOf(pathSeparatorChar) == policyValue.length() - 1) ?
					policyValue.substring(0, policyValue.length() - 1) : policyValue;
		}

		@Override
		boolean isMatch(String resourceValue, Map<String, Object> evalContext) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> RecursivePathResourceMatcher.isMatch(resourceValue=" + resourceValue + ", evalContext=" + evalContext + ")");
			}
			final String noSeparator;
			if (getNeedsDynamicEval()) {
				String expandedPolicyValue = getExpandedValue(evalContext);
				noSeparator = expandedPolicyValue != null ? getStringToCompare(expandedPolicyValue) : null;
			} else {
				if (valueWithoutSeparator == null && value != null) {
					valueWithoutSeparator = getStringToCompare(value);
					valueWithSeparator = valueWithoutSeparator + pathSeparatorChar;
				}
				noSeparator = valueWithoutSeparator;
			}

			boolean ret = primaryFunction.apply(resourceValue, noSeparator);

			if (!ret && noSeparator != null) {
				final String withSeparator = getNeedsDynamicEval() ? noSeparator + pathSeparatorChar : valueWithSeparator;

				ret = fallbackFunction.apply(resourceValue, withSeparator);
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== RecursivePathResourceMatcher.isMatch(resourceValue=" + resourceValue + ", expandedValueWithoutTrailingSeparatorChar=" + noSeparator + ") : result:[" + ret + "]");
			}

			return ret;
		}

		@Override
		public boolean isPrefixMatch(String resourceValue, Map<String, Object> evalContext) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> RecursiveWildcardResourceMatcher.isPrefixMatch(resourceValue=" + resourceValue + ", evalContext=" + evalContext + ")");
			}

			boolean ret = ResourceMatcher.wildcardPrefixMatch(resourceValue, getExpandedValue(evalContext), ioCase);

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== RecursiveWildcardResourceMatcher.isPrefixMatch(resourceValue=" + resourceValue + ", expandedValue=" + getExpandedValue(evalContext) + ") : result:[" + ret + "]");
			}
			return ret;
		}

		@Override
		public boolean isChildMatch(String resourceValue, Map<String, Object> evalContext) {
			boolean ret = false;
			final String noSeparator;
			if (getNeedsDynamicEval()) {
				String expandedPolicyValue = getExpandedValue(evalContext);
				noSeparator = expandedPolicyValue != null ? getStringToCompare(expandedPolicyValue) : null;
			} else {
				if (valueWithoutSeparator == null && value != null) {
					valueWithoutSeparator = getStringToCompare(value);
					valueWithSeparator = valueWithoutSeparator + pathSeparatorChar;
				}
				noSeparator = valueWithoutSeparator;
			}
			final int lastLevelSeparatorIndex = noSeparator != null ? noSeparator.lastIndexOf(pathSeparatorChar) : -1;

			if (lastLevelSeparatorIndex != -1) {
				final String shorterExpandedValue = noSeparator.substring(0, lastLevelSeparatorIndex);

				if (resourceValue.charAt(resourceValue.length() - 1) == pathSeparatorChar) {
					resourceValue = resourceValue.substring(0, resourceValue.length() - 1);
				}

				ret = primaryFunction.apply(resourceValue, shorterExpandedValue);
			}

			return ret;
		}
	}
}
