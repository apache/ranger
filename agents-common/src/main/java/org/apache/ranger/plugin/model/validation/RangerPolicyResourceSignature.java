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

package org.apache.ranger.plugin.model.validation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;

public class RangerPolicyResourceSignature {

	private static final Log LOG = LogFactory.getLog(RangerPolicyResourceSignature.class);
	static final RangerPolicyResourceSignature _EmptyResourceSignature = new RangerPolicyResourceSignature((RangerPolicy)null);
	
	private final String _string;
	private final String _hash;
	private final RangerPolicy _policy;

	public RangerPolicyResourceSignature(RangerPolicy policy) {
		_policy = policy;
		String asString = getResourceString(_policy);
		if (asString == null) {
			_string = "";
		} else {
			_string = asString;
		}
		_hash = DigestUtils.md5Hex(_string);
	}

	/**
	 * Only added for testability.  Do not make public
	 * @param string
	 */
	RangerPolicyResourceSignature(String string) {
		_policy = null;
		if (string == null) {
			_string = "";
		} else {
			_string = string;
		}
		_hash = DigestUtils.md5Hex(_string);
	}
	
	public String asString() {
		return _string;
	}

	public String asHashHex() {
		return _hash;
	}
	
	@Override
	public int hashCode() {
		// we assume no collision
		return Objects.hashCode(_hash);
	}
	
	@Override
	public boolean equals(Object object) {
		if (object == null || !(object instanceof RangerPolicyResourceSignature)) {
			return false;
		}
		RangerPolicyResourceSignature that = (RangerPolicyResourceSignature)object;
		return Objects.equals(this._hash, that._hash);
	}
	
	@Override
	public String toString() {
		return String.format("%s: %s", _hash, _string);
	}

	String getResourceString(RangerPolicy policy) {
		// invalid/empty policy gets a deterministic signature as if it had an
		// empty resource string
		if (!isPolicyValidForResourceSignatureComputation(policy)) {
			return null;
		}
		Map<String, RangerPolicyResourceView> resources = new TreeMap<String, RangerPolicyResourceView>();
		for (Map.Entry<String, RangerPolicyResource> entry : policy.getResources().entrySet()) {
			String resourceName = entry.getKey();
			RangerPolicyResourceView resourceView = new RangerPolicyResourceView(entry.getValue());
			resources.put(resourceName, resourceView);
		}
		String result = resources.toString();
		return result;
	}

	boolean isPolicyValidForResourceSignatureComputation(RangerPolicy policy) {
		boolean valid = false;
		if (policy == null) {
			LOG.debug("isPolicyValidForResourceSignatureComputation: policy was null!");
		} else if (policy.getResources() == null) {
			LOG.debug("isPolicyValidForResourceSignatureComputation: resources collection on policy was null!");
		} else if (policy.getResources().containsKey(null)) {
			LOG.debug("isPolicyValidForResourceSignatureComputation: resources collection has resource with null name!");
		} else {
			valid = true;
		}
		return valid;
	}

	static class RangerPolicyResourceView {
		final RangerPolicyResource _policyResource;

		RangerPolicyResourceView(RangerPolicyResource policyResource) {
			_policyResource = policyResource;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("{");
			if (_policyResource != null) {
				builder.append("values=");
				if (_policyResource.getValues() != null) {
					List<String> values = new ArrayList<String>(_policyResource.getValues());
					Collections.sort(values);
					builder.append(values);
				}
				builder.append(",excludes=");
				if (_policyResource.getIsExcludes() == null) { // null is same as false
					builder.append(Boolean.FALSE);
				} else {
					builder.append(_policyResource.getIsExcludes());
				}
				builder.append(",recursive=");
				if (_policyResource.getIsRecursive() == null) { // null is the same as false
					builder.append(Boolean.FALSE);
				} else {
					builder.append(_policyResource.getIsRecursive());
				}
			}
			builder.append("}");
			return builder.toString();
		}
	}
}
