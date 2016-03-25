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
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;

import java.text.SimpleDateFormat;
import java.util.*;

public final class RangerScriptExecutionContext {
	private static final Log LOG = LogFactory.getLog(RangerScriptExecutionContext.class);
	public static final String DATETIME_FORMAT_PATTERN = "yyyy/MM/dd";

	private final RangerAccessRequest accessRequest;
	private Boolean result = false;

	RangerScriptExecutionContext(final RangerAccessRequest accessRequest) {
		this.accessRequest = accessRequest;
	}

	public String getResource() {
		String ret = null;
		Object val = getRequestContext().get(RangerAccessRequestUtil.KEY_CONTEXT_RESOURCE);

		if(val != null) {
			if(val instanceof RangerAccessResource) {
				ret = ((RangerAccessResource)val).getAsString();
			} else {
				ret = val.toString();
			}
		}

		return ret;
	}

	public Map<String, Object> getRequestContext() {
		return accessRequest.getContext();
	}

	public String getRequestContextAttribute(String attributeName) {
		String ret = null;

		if (StringUtils.isNotBlank(attributeName)) {
			Object val = getRequestContext().get(attributeName);

			if(val != null) {
				ret = val.toString();
			}
		}

		return ret;
	}

	public boolean isAccessTypeAny() { return accessRequest.isAccessTypeAny(); }

	public boolean isAccessTypeDelegatedAdmin() { return accessRequest.isAccessTypeDelegatedAdmin(); }

	public String getUser() { return accessRequest.getUser(); }

	public Set<String> getUserGroups() { return accessRequest.getUserGroups(); }

	public Date getAccessTime() { return accessRequest.getAccessTime(); }

	public String getClientIPAddress() { return accessRequest.getClientIPAddress(); }

	public String getClientType() { return accessRequest.getClientType(); }

	public String getAction() { return accessRequest.getAction(); }

	public String getRequestData() { return accessRequest.getRequestData(); }

	public String getSessionId() { return accessRequest.getSessionId(); }

	public RangerTag getCurrentTag() {
		RangerTag ret = null;
		Object    val = getRequestContext().get(RangerAccessRequestUtil.KEY_CONTEXT_TAG_OBJECT);

		if(val != null && val instanceof RangerTag) {
			ret = (RangerTag)val;
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("getCurrentTag() - No current TAG object. Script execution must be for resource-based policy.");
			}
		}
		return ret;
	}

	public String getCurrentTagType() {
		RangerTag tagObject = getCurrentTag();
		return (tagObject != null) ? tagObject.getType() : null;
	}

	public Set<String> getAllTagTypes() {
		Set<String>     allTagTypes   = null;
		List<RangerTag> tagObjectList = getAllTags();

		if (CollectionUtils.isNotEmpty(tagObjectList)) {
			for (RangerTag tag : tagObjectList) {
				String tagType = tag.getType();
				if (allTagTypes == null) {
					allTagTypes = new HashSet<String>();
				}
				allTagTypes.add(tagType);
			}
		}

		return allTagTypes;
	}

	public Map<String, String> getTagAttributes(final String tagType) {
		Map<String, String> ret = null;

		if (StringUtils.isNotBlank(tagType)) {
			List<RangerTag> tagObjectList = getAllTags();

			// Assumption: There is exactly one tag with given tagType in the list of tags - may not be true ***TODO***
			// This will get attributes of the first tagType that matches
			if (CollectionUtils.isNotEmpty(tagObjectList)) {
				for (RangerTag tag : tagObjectList) {
					if (tag.getType().equals(tagType)) {
						ret = tag.getAttributes();
						break;
					}
				}
			}
		}

		return ret;
	}

	public Set<String> getAttributeNames(final String tagType) {
		Set<String>         ret        = null;
		Map<String, String> attributes = getTagAttributes(tagType);

		if (attributes != null) {
			ret = attributes.keySet();
		}

		return ret;
	}

	public String getAttributeValue(final String tagType, final String attributeName) {
		String ret = null;

		if (StringUtils.isNotBlank(tagType) || StringUtils.isNotBlank(attributeName)) {
			Map<String, String> attributes = getTagAttributes(tagType);

			if (attributes != null) {
				ret = attributes.get(attributeName);
			}
		}
		return ret;
	}

	public String getAttributeValue(final String attributeName) {
		String ret = null;

		if (StringUtils.isNotBlank(attributeName)) {
			RangerTag tag = getCurrentTag();
			Map<String, String> attributes = null;
			if (tag != null) {
				attributes = tag.getAttributes();
			}
			if (attributes != null) {
				ret = attributes.get(attributeName);
			}
		}

		return ret;
	}

	public boolean getResult() {
		return result;

	}

	public void setResult(final boolean result) {
		this.result = result;
	}

	// Utilities - TODO

	public Date getAsDate(String value) {
		Date ret = null;

		if (StringUtils.isNotBlank(value)) {
			SimpleDateFormat df = new SimpleDateFormat(DATETIME_FORMAT_PATTERN);
			try {
				Date expiryDate = df.parse(value);
				if (expiryDate == null) {
					LOG.error("Could not parse provided expiry_date into a valid date, expiry_date=" + value + ", Format-String=" + DATETIME_FORMAT_PATTERN);
				} else {
					ret = StringUtil.getUTCDateForLocalDate(expiryDate);
				}
			} catch (Exception ex) {
				LOG.error("RangerScriptExecutionContext.getAsDate() - Could not convert " + value + " to Date, exception=" + ex);
			}
		}

		return ret;
	}

	public Date getTagAttributeAsDate(String tagType, String attributeName) {
		String attrValue = getAttributeValue(tagType, attributeName);

		return getAsDate(attrValue);
	}

	public boolean isAccessedAfter(String tagType, String attributeName) {
		boolean ret        = false;
		Date    accessDate = getAccessTime();
		Date    expiryDate = getTagAttributeAsDate(tagType, attributeName);

		if (expiryDate == null || accessDate.after(expiryDate) || accessDate.equals(expiryDate)) {
			ret = true;
		}

		return ret;
	}

	public boolean isAccessedAfter(String attributeName) {
		boolean ret        = false;
		Date    accessDate = getAccessTime();
		Date    expiryDate = getAsDate(getAttributeValue(attributeName));

		if (expiryDate == null || accessDate.after(expiryDate) || accessDate.equals(expiryDate)) {
			ret = true;
		}

		return ret;
	}

	public boolean isAccessedBefore(String tagType, String attributeName) {
		boolean ret        = true;
		Date    accessDate = getAccessTime();
		Date    expiryDate = getTagAttributeAsDate(tagType, attributeName);

		if (expiryDate == null || accessDate.after(expiryDate)) {
			ret = false;
		}

		return ret;
	}

	public boolean isAccessedBefore(String attributeName) {
		boolean ret        = true;
		Date    accessDate = getAccessTime();
		Date    expiryDate = getAsDate(getAttributeValue(attributeName));

		if (expiryDate == null || accessDate.after(expiryDate)) {
			ret = false;
		}

		return ret;
	}

	private List<RangerTag> getAllTags() {
		List<RangerTag> ret = RangerAccessRequestUtil.getRequestTagsFromContext(accessRequest.getContext());
		
		if(ret == null) {
			if (LOG.isDebugEnabled()) {
				String resource = accessRequest.getResource().getAsString();

				LOG.debug("getAllTags() - No TAGS. No TAGS for the RangerAccessResource=" + resource);
			}
		}

		return ret;
	}

	public void logDebug(String msg) {
		LOG.debug(msg);
	}

	public void logInfo(String msg) {
		LOG.info(msg);
	}

	public void logWarn(String msg) {
		LOG.warn(msg);
	}

	public void logError(String msg) {
		LOG.error(msg);
	}

	public void logFatal(String msg) {
		LOG.fatal(msg);
	}
}
