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

package org.apache.ranger.plugin.util;

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.ranger.plugin.contextenricher.RangerTagForEval;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerAccessRequestUtil {
	private static final Logger LOG = LoggerFactory.getLogger(RangerAccessRequestUtil.class);

	public static final String KEY_CONTEXT_TAGS                             = "TAGS";
	public static final String KEY_CONTEXT_TAG_OBJECT                       = "TAG_OBJECT";
	public static final String KEY_CONTEXT_RESOURCE                         = "RESOURCE";
	public static final String KEY_CONTEXT_REQUESTED_RESOURCES              = "REQUESTED_RESOURCES";
	public static final String KEY_CONTEXT_USERSTORE                        = "USERSTORE";
	public static final String KEY_TOKEN_NAMESPACE                          = "token:";
	public static final String KEY_USER                                     = "USER";
	public static final String KEY_OWNER                                    = "OWNER";
	public static final String KEY_ROLES                                    = "ROLES";
	public static final String KEY_CONTEXT_IS_ANY_ACCESS                    = "ISANYACCESS";
	public static final String KEY_CONTEXT_ALL_ACCESSTYPES 	                = "ALLACCESSTYPES";
	public static final String KEY_CONTEXT_IGNORE_IF_NOT_DENIED_ACCESSTYPES = "IGNOREIFNOTDENIEDACCESSTYPES";
	public static final String KEY_CONTEXT_ALL_ACCESS_TYPE_RESULTS          = "ALL_ACCESS_TYPE_RESULTS";
	public static final String KEY_CONTEXT_REQUEST                          = "_REQUEST";
	public static final String KEY_CONTEXT_IS_REQUEST_PREPROCESSED          = "ISREQUESTPREPROCESSED";
	public static final String KEY_CONTEXT_RESOURCE_ZONE_NAMES              = "RESOURCE_ZONE_NAMES";
	public static final String KEY_CONTEXT_ALL_ACCESSTYPE_GROUPS            = "ALLACCESSTYPEGROUPS";
	public static final String KEY_CONTEXT_ALL_ACCESS_TYPE_ACL_RESULTS      = "ALL_ACCESS_TYPE_ACL_RESULTS";


	public static void setRequestTagsInContext(Map<String, Object> context, Set<RangerTagForEval> tags) {
		if (CollectionUtils.isEmpty(tags)) {
			context.remove(KEY_CONTEXT_TAGS);
		} else {
			context.put(KEY_CONTEXT_TAGS, tags);
		}
	}

	public static Set<RangerTagForEval> getRequestTagsFromContext(Map<String, Object> context) {
		Set<RangerTagForEval> ret = null;
		Object                val = context.get(RangerAccessRequestUtil.KEY_CONTEXT_TAGS);

		if (val instanceof Set<?>) {
			try {
				@SuppressWarnings("unchecked") Set<RangerTagForEval> tags = (Set<RangerTagForEval>) val;

				ret = tags;
			} catch (Throwable t) {
				LOG.error("getRequestTags(): failed to get tags from context", t);
			}
		}

		return ret;
	}

	public static void setCurrentTagInContext(Map<String, Object> context, RangerTagForEval tag) {
		context.put(KEY_CONTEXT_TAG_OBJECT, tag);
	}

	public static RangerTagForEval getCurrentTagFromContext(Map<String, Object> context) {
		RangerTagForEval ret = null;
		Object           val = context.get(KEY_CONTEXT_TAG_OBJECT);

		if (val instanceof RangerTagForEval) {
			ret = (RangerTagForEval) val;
		}

		return ret;
	}

	public static void setRequestedResourcesInContext(Map<String, Object> context, RangerRequestedResources resources) {
		context.put(KEY_CONTEXT_REQUESTED_RESOURCES, resources);
	}

	public static RangerRequestedResources getRequestedResourcesFromContext(Map<String, Object> context) {
		RangerRequestedResources ret = null;
		Object                   val = context.get(KEY_CONTEXT_REQUESTED_RESOURCES);

		if (val instanceof RangerRequestedResources) {
			ret = (RangerRequestedResources) val;
		}

		return ret;
	}

	public static void setCurrentResourceInContext(Map<String, Object> context, RangerAccessResource resource) {
		context.put(KEY_CONTEXT_RESOURCE, resource);
	}

	public static RangerAccessResource getCurrentResourceFromContext(Map<String, Object> context) {
		RangerAccessResource ret = null;
		Object               val = MapUtils.isNotEmpty(context) ? context.get(KEY_CONTEXT_RESOURCE) : null;

		if (val instanceof RangerAccessResource) {
			ret = (RangerAccessResource) val;
		}

		return ret;
	}

	public static Map<String, Object> copyContext(Map<String, Object> context) {
		final Map<String, Object> ret;

		if (MapUtils.isEmpty(context)) {
			ret = new HashMap<>();
		} else {
			ret = new HashMap<>(context);

			ret.remove(KEY_CONTEXT_TAGS);
			ret.remove(KEY_CONTEXT_TAG_OBJECT);
			ret.remove(KEY_CONTEXT_RESOURCE);
			ret.remove(KEY_CONTEXT_REQUEST);
			ret.remove(KEY_CONTEXT_IS_ANY_ACCESS);
			ret.remove(KEY_CONTEXT_ALL_ACCESSTYPES);
			ret.remove(KEY_CONTEXT_ALL_ACCESSTYPE_GROUPS);
			ret.remove(KEY_CONTEXT_ALL_ACCESS_TYPE_RESULTS);
			ret.remove(KEY_CONTEXT_ALL_ACCESS_TYPE_ACL_RESULTS);
			ret.remove(KEY_CONTEXT_IS_REQUEST_PREPROCESSED);
			ret.remove(KEY_CONTEXT_IGNORE_IF_NOT_DENIED_ACCESSTYPES);
			// don't remove REQUESTED_RESOURCES
		}

		return ret;
	}

	public static void setCurrentUserInContext(Map<String, Object> context, String user) {
		setTokenInContext(context, KEY_USER, user);
	}

	public static void setOwnerInContext(Map<String, Object> context, String owner) {
		setTokenInContext(context, KEY_OWNER, owner);
	}

	public static String getCurrentUserFromContext(Map<String, Object> context) {
		Object ret = getTokenFromContext(context, KEY_USER);
		return ret != null ? ret.toString() : "";
	}

	public static void setTokenInContext(Map<String, Object> context, String tokenName, Object tokenValue) {
		String tokenNameWithNamespace = KEY_TOKEN_NAMESPACE + tokenName;
		context.put(tokenNameWithNamespace, tokenValue);
	}

	public static Object getTokenFromContext(Map<String, Object> context, String tokenName) {
		String tokenNameWithNamespace = KEY_TOKEN_NAMESPACE + tokenName;
		return MapUtils.isNotEmpty(context) ? context.get(tokenNameWithNamespace) : null;
	}

	public static void setCurrentUserRolesInContext(Map<String, Object> context, Set<String> roles) {
		setTokenInContext(context, KEY_ROLES, roles);
	}

	public static Set<String> getCurrentUserRolesFromContext(Map<String, Object> context) {
		Object ret = getTokenFromContext(context, KEY_ROLES);
		return ret != null ? (Set<String>) ret : Collections.EMPTY_SET;
	}

	public static Set<String> getUserRoles(RangerAccessRequest request) {
		Set<String> ret = Collections.EMPTY_SET;

		if (request != null) {
			ret = request.getUserRoles();

			if (CollectionUtils.isEmpty(ret)) {
				ret = RangerAccessRequestUtil.getCurrentUserRolesFromContext(request.getContext());
			}
		}

		return ret;
	}

	public static void setRequestUserStoreInContext(Map<String, Object> context, RangerUserStore rangerUserStore) {
		context.put(KEY_CONTEXT_USERSTORE, rangerUserStore);
	}

	public static RangerUserStore getRequestUserStoreFromContext(Map<String, Object> context) {
		RangerUserStore ret = null;
		Object          val = context.get(KEY_CONTEXT_USERSTORE);

		if (val instanceof RangerUserStore) {
			ret = (RangerUserStore) val;
		}

		return ret;
	}

	public static void setIsAnyAccessInContext(Map<String, Object> context, Boolean value) {
		context.put(KEY_CONTEXT_IS_ANY_ACCESS, value);
	}

	public static boolean getIsAnyAccessInContext(Map<String, Object> context) {
		Boolean value = (Boolean) context.get(KEY_CONTEXT_IS_ANY_ACCESS);
		return value != null && value;
	}

	public static void setIsRequestPreprocessed(Map<String, Object> context, Boolean value) {
		context.put(KEY_CONTEXT_IS_REQUEST_PREPROCESSED, value);
	}

	public static boolean getIsRequestPreprocessed(Map<String, Object> context) {
		Boolean value = (Boolean) context.get(KEY_CONTEXT_IS_REQUEST_PREPROCESSED);
		return value != null && value;
	}

	public static void setAllRequestedAccessTypes(Map<String, Object> context, Set<String> accessTypes) {
		context.put(KEY_CONTEXT_ALL_ACCESSTYPES, accessTypes);
	}

	public static void setIgnoreIfNotDeniedAccessTypes(Map<String, Object> context, Set<String> accessTypes) {
		context.put(KEY_CONTEXT_IGNORE_IF_NOT_DENIED_ACCESSTYPES, accessTypes);
	}

	public static Set<String> getIgnoreIfNotDeniedAccessTypes(RangerAccessRequest request) {
		Set<String> ret = Collections.emptySet();

		Object val = request.getContext().get(KEY_CONTEXT_IGNORE_IF_NOT_DENIED_ACCESSTYPES);

		if (val != null) {
			if (val instanceof Set<?>) {
				ret = (Set<String>) val;
			} else if (val instanceof List<?>) {
				ret = new TreeSet<>((List<String>) val);
			} else {
				LOG.error("getNotDeniedRequestedAccessTypes(): failed to get NOTDENIEDACCESSTYPES from context");
			}
		}
		return ret;
	}


	@SuppressWarnings("unchecked")
	public static Set<String> getAllRequestedAccessTypes(RangerAccessRequest request) {
		Set<String> ret = null;

		Object val = request.getContext().get(KEY_CONTEXT_ALL_ACCESSTYPES);

		if (val != null) {
			if (val instanceof Set<?>) {
				ret = (Set<String>) val;
			} else if (val instanceof List<?>) {
				ret = new TreeSet<>((List<String>) val);
			} else {
				LOG.error("getAllRequestedAccessTypes(): failed to get ALLACCESSTYPES from context");
			}
		}

		return ret != null ? ret : Collections.singleton(request.getAccessType());
	}

	public static Set<Set<String>> getAllRequestedAccessTypeGroups(RangerAccessRequest request) {
		Object val = request.getContext().get(KEY_CONTEXT_ALL_ACCESSTYPE_GROUPS);
		return (Set<Set<String>>) val;
	}

	public static void setAllRequestedAccessTypeGroups(RangerAccessRequest request, Set<Set<String>> accessTypeGroups) {
		if (accessTypeGroups == null || accessTypeGroups.isEmpty()) {
			request.getContext().put(KEY_CONTEXT_ALL_ACCESSTYPE_GROUPS, new TreeSet<>());
		} else {
			request.getContext().put(KEY_CONTEXT_ALL_ACCESSTYPE_GROUPS, accessTypeGroups);
		}
	}

	public static Map<String, RangerAccessResult> getAccessTypeResults(RangerAccessRequest request) {
		Map<String, RangerAccessResult> ret;
		Object                          val = request.getContext().get(KEY_CONTEXT_ALL_ACCESS_TYPE_RESULTS);
		if (val == null) {
			ret = new HashMap<>();
			request.getContext().put(KEY_CONTEXT_ALL_ACCESS_TYPE_RESULTS, ret);
		} else {
			ret = (Map<String, RangerAccessResult>) val;
		}
		return ret;
	}

	public static Map<String, Integer> getAccessTypeACLResults(RangerAccessRequest request) {
		Map<String, Integer> ret;
		Object               val = request.getContext().get(KEY_CONTEXT_ALL_ACCESS_TYPE_ACL_RESULTS);
		if (val == null) {
			ret = new HashMap<>();
			request.getContext().put(KEY_CONTEXT_ALL_ACCESS_TYPE_ACL_RESULTS, ret);
		} else {
			ret = (Map<String, Integer>) val;
		}
		return ret;
	}

	public static void setRequestInContext(RangerAccessRequest request) {
		Map<String, Object> context = request.getContext();

		if (context != null) {
			context.put(KEY_CONTEXT_REQUEST, request);
		}
	}

	public static RangerAccessRequest getRequestFromContext(Map<String, Object> context) {
		RangerAccessRequest ret = null;

		if (context != null) {
			Object val = context.get(KEY_CONTEXT_REQUEST);

			if (val != null) {
				if (val instanceof RangerAccessRequest) {
					ret = (RangerAccessRequest) val;
				} else {
					LOG.error("getRequestFromContext(): expected RangerAccessRequest, but found " + val.getClass().getCanonicalName());
				}
			}
		}

		return ret;
	}

	public static void setResourceZoneNamesInContext(RangerAccessRequest request, Set<String> zoneNames) {
		Map<String, Object> context = request.getContext();

		if (context != null) {
			context.put(KEY_CONTEXT_RESOURCE_ZONE_NAMES, zoneNames);
		} else {
			LOG.error("setResourceZoneNamesInContext({}): context is null", request);
		}
	}

	@SuppressWarnings("unchecked")
	public static Set<String> getResourceZoneNamesFromContext(Map<String, Object> context) {
		Set<String> ret = null;

		if (context != null) {
			Object val = context.get(KEY_CONTEXT_RESOURCE_ZONE_NAMES);

			if (val instanceof Set) {
				ret = (Set<String>) val;
			} else {
				if (val != null) {
					LOG.error("getResourceZoneNamesFromContext(): expected Set<String>, but found {}", val.getClass().getCanonicalName());
				}
			}
		}

		return ret;
	}

	public static String getResourceZoneNameFromContext(Map<String, Object> context) {
		Set<String> ret = getResourceZoneNamesFromContext(context);

		return ret != null && ret.size() == 1 ? ret.iterator().next() : null;
	}

	public static void setAccessTypeResults(Map<String, Object> context, Map<String, RangerAccessResult> accessTypeResults) {
		if (context != null) {
			if (accessTypeResults != null) {
				context.put(KEY_CONTEXT_ALL_ACCESS_TYPE_RESULTS, accessTypeResults);
			} else {
				context.remove(KEY_CONTEXT_ALL_ACCESS_TYPE_RESULTS);
			}
		}
	}

	public static void setAccessTypeACLResults(Map<String, Object> context, Map<String, Integer> accessTypeResults) {
		if (context != null) {
			if (accessTypeResults != null) {
				context.put(KEY_CONTEXT_ALL_ACCESS_TYPE_ACL_RESULTS, accessTypeResults);
			} else {
				context.remove(KEY_CONTEXT_ALL_ACCESS_TYPE_ACL_RESULTS);
			}
		}
	}

	public static Map<String, RangerAccessResult> getAccessTypeResults(Map<String, Object> context) {
		Map<String, RangerAccessResult> ret = null;

		if (context != null) {
			Object o = context.get(KEY_CONTEXT_ALL_ACCESS_TYPE_RESULTS);
			if (o != null) {
				ret = (Map<String, RangerAccessResult>) o;
			}
		}

		return ret;
	}

	public static void setAccessTypeResult(Map<String, Object> context, String accessType, RangerAccessResult result) {
		if (context != null) {
			Map<String, RangerAccessResult> results = getAccessTypeResults(context);

			if (results == null) {
				results = new HashMap<>();

				context.put(KEY_CONTEXT_ALL_ACCESS_TYPE_RESULTS, results);
			}

			results.putIfAbsent(accessType, result);
		}
	}
}
