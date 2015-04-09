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


public class RangerDefaultResourceMatcher extends RangerAbstractResourceMatcher {
	private static final Log LOG = LogFactory.getLog(RangerDefaultResourceMatcher.class);

	@Override
	public boolean isMatch(String resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultResourceMatcher.isMatch(" + resource + ")");
		}

		boolean ret = false;

		if(resource == null || isMatchAny) {
			ret = isMatchAny;
		} else {
			if(optIgnoreCase) {
				resource = resource.toLowerCase();
			}

			for(String policyValue : policyValues) {
				ret = optWildCard ? FilenameUtils.wildcardMatch(resource, policyValue) : StringUtils.equals(resource, policyValue);

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

		sb.append("}");

		return sb;
	}
}
