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

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;


public class RangerPathResourceMatcher extends RangerAbstractResourceMatcher {
	private static final Log LOG = LogFactory.getLog(RangerPathResourceMatcher.class);

	private boolean policyIsRecursive = false;

	@Override
	public void init(RangerResourceDef resourceDef, RangerPolicyResource policyResource, String optionsString) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPathResourceMatcher.init(" + resourceDef + ", " + policyResource + ", " + optionsString + ")");
		}

		super.init(resourceDef, policyResource,  optionsString);

		policyIsRecursive = policyResource == null ? false : policyResource.getIsRecursive();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPathResourceMatcher.init(" + resourceDef + ", " + policyResource + ", " + optionsString + ")");
		}
	}

	@Override
	public boolean isMatch(String resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPathResourceMatcher.isMatch(" + resource + ")");
		}

		boolean ret = false;

		if(resource != null) {
			if(optIgnoreCase) {
				resource = resource.toLowerCase();
			}

			for(String policyValue : policyValues) {
				if(policyIsRecursive) {
					ret = StringUtils.startsWith(resource, policyValue);
					
					if(! ret && optWildCard) {
						ret = isRecursiveWildCardMatch(resource, policyValue) ;
					}
				} else {
					ret = StringUtils.equals(resource, policyValue);
					
					if(! ret && optWildCard) {
						ret = FilenameUtils.wildcardMatch(resource, policyValue);
					}
				}

				if(ret) {
					break;
				}
			}
		} else {
			ret = isMatchAny;
		}

		if(policyIsExcludes) {
			ret = !ret;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPathResourceMatcher.isMatch(" + resource + "): " + ret);
		}

		return ret;
	}
	
	private static boolean isRecursiveWildCardMatch(String pathToCheck, String wildcardPath) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPathResourceMatcher.isRecursiveWildCardMatch(" + pathToCheck + ", " + wildcardPath + ")");
		}

		boolean ret = false;

		if (pathToCheck != null) {
			StringBuilder sb = new StringBuilder() ;

			for(String p : pathToCheck.split(org.apache.hadoop.fs.Path.SEPARATOR) ) {
				sb.append(p);

				boolean matchFound = FilenameUtils.wildcardMatch(sb.toString(), wildcardPath) ;

				if (matchFound) {
					ret = true ;

					break;
				}

				sb.append(org.apache.hadoop.fs.Path.SEPARATOR) ;
			}

			sb = null;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPathResourceMatcher.isRecursiveWildCardMatch(" + pathToCheck + ", " + wildcardPath + "): " + ret);
		}

		return ret;
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerPathResourceMatcher={");

		super.toString(sb);

		sb.append("policyValues={");
		if(policyValues != null) {
			for(String value : policyValues) {
				sb.append(value).append(",");
			}
		}
		sb.append("} ");

		sb.append("policyIsExcludes={").append(policyIsExcludes).append("} ");
		sb.append("policyIsRecursive={").append(policyIsRecursive).append("} ");

		sb.append("}");

		return sb;
	}
}
