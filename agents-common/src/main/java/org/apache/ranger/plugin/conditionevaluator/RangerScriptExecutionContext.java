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

package org.apache.ranger.plugin.conditionevaluator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;

import java.text.SimpleDateFormat;
import java.util.*;

public final class RangerScriptExecutionContext {
	private static final Log LOG = LogFactory.getLog(RangerScriptExecutionContext.class);
	public static final String DATETIME_FORMAT_PATTERN = "EEE MMM dd HH:mm:ss z yyyy";

	private final RangerAccessRequest accessRequest;
	private Boolean result = false;

	RangerScriptExecutionContext(final RangerAccessRequest accessRequest) {
		this.accessRequest = accessRequest;
	}

	public final String getResource() {

		@SuppressWarnings("unchecked")
		RangerAccessResource resource  = (RangerAccessResource)getEvaluationContext().get(RangerPolicyEngine.KEY_CONTEXT_RESOURCE);

		return resource != null ? resource.getAsString() : null;
	}

	public final Map<String, Object> getEvaluationContext() {
		return accessRequest.getContext();
	}

	public final boolean isAccessTypeAny() { return accessRequest.isAccessTypeAny(); }

	public final boolean isAccessTypeDelegatedAdmin() { return accessRequest.isAccessTypeDelegatedAdmin(); }

	public final String getUser() { return accessRequest.getUser(); }

	public final Set<String> getUserGroups() { return accessRequest.getUserGroups(); }

	public final Date getAccessTime() { return accessRequest.getAccessTime(); }

	public final String getClientIPAddress() { return accessRequest.getClientIPAddress(); }

	public final String getClientType() { return accessRequest.getClientType(); }

	public final String getAction() { return accessRequest.getAction(); }

	public final String getRequestData() { return accessRequest.getRequestData(); }

	public final String getSessionId() { return accessRequest.getSessionId(); }

	public final RangerTag getCurrentTag() {
		@SuppressWarnings("unchecked")
		RangerTag tagObject = (RangerTag)getEvaluationContext()
				.get(RangerPolicyEngine.KEY_CONTEXT_TAG_OBJECT);
		if (tagObject == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("getCurrentTag() - No current TAG object. Script execution must be for resource-based policy.");
			}
		}
		return tagObject;
	}

	public final String getCurrentTagName() {
		RangerTag tagObject = getCurrentTag();
		return (tagObject != null) ? tagObject.getName() : null;
	}

	public final Set<String> getAllTagNames() {

		Set<String> allTagNames = null;

		List<RangerTag> tagObjectList = getAllTags();

		if (CollectionUtils.isNotEmpty(tagObjectList)) {

			for (RangerTag tag : tagObjectList) {
				String tagName = tag.getName();
				if (allTagNames == null) {
					allTagNames = new HashSet<String>();
				}
				allTagNames.add(tagName);
			}
		}

		return allTagNames;
	}

	public final Map<String, String> getTagAttributeValues(final String tagName) {

		Map<String, String> ret = null;

		if (StringUtils.isNotBlank(tagName)) {

			List<RangerTag> tagObjectList = getAllTags();

			// Assumption: There is exactly one tag with given tagName in the list of tags - may not be true ***TODO***
			// This will get attributeValues of the first tagName that matches

			if (CollectionUtils.isNotEmpty(tagObjectList)) {
				for (RangerTag tag : tagObjectList) {
					if (tag.getName().equals(tagName)) {
						ret = tag.getAttributeValues();
						break;
					}
				}
			}
		}

		return ret;
	}

	public final Set<String> getAttributeNames(final String tagName) {

		Set<String> ret = null;

		Map<String, String> attributeValues = getTagAttributeValues(tagName);

		if (attributeValues != null) {
			ret = attributeValues.keySet();
		}

		return ret;
	}

	public final String getAttributeValue(final String tagName, final String attributeName) {

		String ret = null;
		Map<String, String> attributeValues;

		if (StringUtils.isNotBlank(tagName) || StringUtils.isNotBlank(attributeName)) {
			attributeValues = getTagAttributeValues(tagName);

			if (attributeValues != null) {
				ret = attributeValues.get(attributeName);
			}
		}
		return ret;
	}

	public final String getAttributeValue(final String attributeName) {

		String ret = null;

		if (StringUtils.isNotBlank(attributeName)) {
			RangerTag tag = getCurrentTag();
			Map<String, String> attributeValues = null;
			if (tag != null) {
				attributeValues = tag.getAttributeValues();
			}
			if (attributeValues != null) {
				ret = attributeValues.get(attributeName);
			}
		}
		return ret;
	}

	public final boolean getResult() {
		return result;

	}

	public final void setResult(final boolean result) {
		this.result = result;
	}

	// Utilities - TODO

	public final Date getAsDate(String value) {

		Date ret = null;

		if (StringUtils.isNotBlank(value)) {
			SimpleDateFormat df = new SimpleDateFormat(DATETIME_FORMAT_PATTERN);
			try {
				ret = df.parse(value);
			} catch (Exception ex) {
				LOG.error("RangerScriptExecutionContext.getAsDate() - Could not convert " + value + " to Date, exception=" + ex);
			}
		}

		return ret;
	}

	public final Date getTagAttributeAsDate(String tagName, String attributeName) {
		// sample JavaScript to demonstrate use of this helper method

		/*

		importPackage(java.util);
		var expiryDate = ctx.getTagAttributeAsDate('PII', 'expiryDate')
		var now = new Date();
		now.getTime() < expiryDate.getTime());"

		*/

		String attrValue = getAttributeValue(tagName, attributeName);

		return getAsDate(attrValue);

	}

	public final boolean isAccessedAfter(String tagName, String attributeName) {

		boolean ret = false;

		Date accessDate = getAccessTime();

		Date expiryDate = getTagAttributeAsDate(tagName, attributeName);

		if (expiryDate == null || accessDate.after(expiryDate) || accessDate.equals(expiryDate)) {
			ret = true;
		}

		return ret;
	}

	public final boolean isAccessedAfter(String attributeName) {

		boolean ret = false;

		Date accessDate = getAccessTime();

		Date expiryDate = getAsDate(getAttributeValue(attributeName));

		if (expiryDate == null || accessDate.after(expiryDate) || accessDate.equals(expiryDate)) {
			ret = true;
		}

		return ret;
	}

	public final boolean isAccessedBefore(String tagName, String attributeName) {

		boolean ret = true;

		Date accessDate = getAccessTime();

		Date expiryDate = getTagAttributeAsDate(tagName, attributeName);

		if (expiryDate == null || accessDate.after(expiryDate)) {
			ret = false;
		}

		return ret;
	}

	public final boolean isAccessedBefore(String attributeName) {

		boolean ret = true;

		Date accessDate = getAccessTime();

		Date expiryDate = getAsDate(getAttributeValue(attributeName));

		if (expiryDate == null || accessDate.after(expiryDate)) {
			ret = false;
		}

		return ret;
	}

	private List<RangerTag> getAllTags() {

		@SuppressWarnings("unchecked")
		List<RangerTag> ret = (List<RangerTag>)getEvaluationContext().get(RangerPolicyEngine.KEY_CONTEXT_TAGS);

		if (ret == null) {
			if (LOG.isDebugEnabled()) {
				String resource = accessRequest.getResource().getAsString();

				LOG.debug("getAllTags() - No current TAGS. No TAGS for the RangerAccessResource=" + resource);
			}
		}
		return ret;
	}

	public final String getGeolocation(String attributeName) {
		String ret = null;

		if (StringUtils.isNotBlank(attributeName)) {
			ret = (String) getEvaluationContext().get(attributeName);
		}
		return ret;
	}
}
