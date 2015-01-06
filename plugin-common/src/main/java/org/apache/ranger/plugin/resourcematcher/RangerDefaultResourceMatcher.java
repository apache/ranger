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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;


public class RangerDefaultResourceMatcher extends RangerAbstractResourceMatcher {
	private static final Log LOG = LogFactory.getLog(RangerDefaultResourceMatcher.class);

	private List<String> policyValues     = null;
	private boolean      policyIsExcludes = false;

	@Override
	public void init(RangerPolicyResource policyResource, String optionsString) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultResourceMatcher.init(" + policyResource + ", " + optionsString + ")");
		}

		super.init(policyResource,  optionsString);

		policyValues     = new ArrayList<String>();
		policyIsExcludes = false;

		if(policyResource != null) {
			policyIsExcludes = policyResource.getIsExcludes();

			if(policyResource.getValues() != null) {
				for(String policyValue : policyResource.getValues()) {
					if(policyValue == null) {
						continue;
					}

					if(optIgnoreCase) {
						policyValue = policyValue.toLowerCase();
					}

					if(optWildCard) {
						policyValue = getWildCardPattern(policyValue);
					}

					policyValues.add(policyValue);
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultResourceMatcher.init(" + policyResource + ", " + optionsString + ")");
		}
	}

	@Override
	public boolean isMatch(String resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultResourceMatcher.isMatch(" + resource + ")");
		}

		boolean ret = false;

		if(resource != null) {
			if(optIgnoreCase) {
				resource = resource.toLowerCase();
			}

			for(String policyValue : policyValues) {
				ret = optWildCard ? resource.matches(policyValue) : StringUtils.equals(resource, policyValue);

				if(ret) {
					break;
				}
			}
		}

		if(policyIsExcludes) {
			ret = !ret;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultResourceMatcher.isMatch(" + resource + "): " + ret);
		}

		return ret;
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerDefaultResourceMatcher={");

		super.toString(sb);

		sb.append("policyValues={");
		if(policyValues != null) {
			for(String value : policyValues) {
				sb.append(value).append(",");
			}
		}
		sb.append("} ");

		sb.append("policyIsExcludes={").append(policyIsExcludes).append("} ");

		sb.append("}");

		return sb;
	}
}
