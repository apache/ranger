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

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SearchFilter {
    public static final String SERVICE_TYPE             = "serviceType";   // search, sort
    public static final String SERVICE_TYPE_ID          = "serviceTypeId"; // search, sort
    public static final String SERVICE_NAME             = "serviceName";   // search, sort
    public static final String SERVICE_ID               = "serviceId";     // search, sort
    public static final String POLICY_NAME              = "policyName";    // search, sort
    public static final String POLICY_ID                = "policyId";      // search, sort
    public static final String IS_ENABLED               = "isEnabled";     // search
    public static final String IS_RECURSIVE             = "isRecursive";   // search
    public static final String TAG_SERVICE_NAME         = "tagServiceName";  // search
    public static final String TAG_SERVICE_NAME_PARTIAL = "tagServiceNamePartial";  // search
    public static final String TAG_SERVICE_ID           = "tagServiceId";  // search
    public static final String GDS_SERVICE_NAME         = "gdsServiceName";  // search
    public static final String GDS_SERVICE_ID           = "gdsServiceId";  // search
    public static final String USER                     = "user";          // search
    public static final String GROUP                    = "group";         // search
    public static final String ROLE                     = "role";         // search
    public static final String RESOURCE_PREFIX          = "resource:";     // search
    public static final String RESOURCE_MATCH_SCOPE     = "resourceMatchScope"; // search - valid values: "self", "ancestor", "self_or_ancestor"
    public static final String POL_RESOURCE             = "polResource";   // search
    public static final String POLICY_NAME_PARTIAL      = "policyNamePartial";    // search, sort
    public static final String CREATE_TIME              = "createTime";    // sort
    public static final String UPDATE_TIME              = "updateTime";    // sort
    public static final String START_INDEX              = "startIndex";
    public static final String PAGE_SIZE                = "pageSize";
    public static final String SORT_BY                  = "sortBy";
    public static final String RESOURCE_SIGNATURE       = "resourceSignature:";     // search
    public static final String POLICY_TYPE              = "policyType";    // search
    public static final String POLICY_PRIORITY          = "policyPriority";    // search
    public static final String GUID                     = "guid"; //search
    public static final String POLICY_LABEL             = "policyLabel";    // search
    public static final String POLICY_LABELS_PARTIAL    = "policyLabelsPartial";    // search
    public static final String POLICY_LABEL_ID          = "policyLabelId";      // search, sort
    public static final String ZONE_ID                  = "zoneId";      // search, sort
    public static final String ZONE_NAME                = "zoneName";      // search, sort
    public static final String ZONE_NAME_PARTIAL        = "zoneNamePartial";      // search, sort
    public static final String NOT_ZONE_NAME            = "notZoneName";   // search
    public static final String ROLE_ID                  = "roleId";      // search, sort
    public static final String ROLE_NAME                = "roleName";      // search, sort
    public static final String GROUP_NAME               = "groupName";      // search, sort
    public static final String USER_NAME                = "userName";      // search, sort
    public static final String ROLE_NAME_PARTIAL        = "roleNamePartial";      // search
    public static final String GROUP_NAME_PARTIAL       = "groupNamePartial";     // search
    public static final String USER_NAME_PARTIAL        = "userNamePartial";      // search
    public static final String SERVICE_NAME_PREFIX      = "serviceNamePrefix";    // search
    public static final String ZONE_NAME_PREFIX         = "zoneNamePrefix";       // search
    public static final String POLICY_NAME_PREFIX       = "policyNamePrefix";

    public static final String TAG_DEF_ID                = "tagDefId";            // search
    public static final String TAG_DEF_GUID              = "tagDefGuid";          // search
    public static final String TAG_NAMES                 = "tagNames";            // search
    public static final String TAG_TYPE                  = "tagType";             // search
    public static final String TAG_TYPE_PARTIAL          = "tagTypePartial";      // search
    public static final String TAG_SOURCE                = "tagSource";           // search
    public static final String TAG_SOURCE_PARTIAL        = "tagSourcePartial";    // search
    public static final String TAG_ID                    = "tagId";               // search
    public static final String TAG_IDS                   = "tagIds";               // search
    public static final String TAG_GUID                  = "tagGuid";             // search
    public static final String TAG_RESOURCE_ID           = "resourceId";          // search
    public static final String TAG_RESOURCE_IDS          = "resourceIds";          // search
    public static final String TAG_RESOURCE_GUID         = "resourceGuid";        // search
    public static final String TAG_RESOURCE_SERVICE_NAME = "resourceServiceName"; // search
    public static final String TAG_RESOURCE_SIGNATURE    = "resourceSignature";   // search
    public static final String TAG_RESOURCE_ELEMENTS     = "resourceElements";   // search
    public static final String TAG_MAP_ID                = "tagResourceMapId";    // search
    public static final String TAG_MAP_GUID              = "tagResourceMapGuid";  // search

    public static final String SERVICE_NAME_PARTIAL = "serviceNamePartial";

    public static final String PLUGIN_HOST_NAME           = "pluginHostName";
    public static final String PLUGIN_APP_TYPE            = "pluginAppType";
    public static final String PLUGIN_ENTITY_TYPE         = "pluginEntityType";
    public static final String PLUGIN_IP_ADDRESS          = "pluginIpAddress";
    public static final String CLUSTER_NAME               = "clusterName";
    public static final String FETCH_ZONE_UNZONE_POLICIES = "fetchZoneAndUnzonePolicies";
    public static final String FETCH_TAG_POLICIES         = "fetchTagPolicies";
    public static final String FETCH_ZONE_NAME            = "zoneName";
    public static final String FETCH_DENY_CONDITION       = "denyCondition";

    public static final String SERVICE_DISPLAY_NAME         = "serviceDisplayName";            // search, sort
    public static final String SERVICE_DISPLAY_NAME_PARTIAL = "serviceDisplayNamePartial";    // search
    public static final String SERVICE_TYPE_DISPLAY_NAME    = "serviceTypeDisplayName";        // search, sort

    public static final String DATASET_NAME                 = "datasetName";          // search, sort
    public static final String DATASET_NAME_PARTIAL         = "datasetNamePartial";   // search, sort
    public static final String DATASET_ID                   = "datasetId";            // search, sort
    public static final String DATASET_LABEL                = "datasetLabel";
    public static final String DATASET_KEYWORD              = "datasetKeyword";
    public static final String PROJECT_NAME                 = "projectName";          // search, sort
    public static final String PROJECT_NAME_PARTIAL         = "projectNamePartial";   // search, sort
    public static final String PROJECT_ID                   = "projectId";            // search, sort
    public static final String DATA_SHARE_NAME              = "dataShareName";        // search, sort
    public static final String DATA_SHARE_NAME_PARTIAL      = "dataShareNamePartial"; // search, sort
    public static final String DATA_SHARE_ID                = "dataShareId";          // search, sort
    public static final String EXCLUDE_DATASET_ID           = "excludeDatasetId";     // search
    public static final String SHARED_RESOURCE_NAME         = "sharedResourceName";   // search, sort
    public static final String SHARED_RESOURCE_NAME_PARTIAL = "sharedResourceNamePartial";   // search, sort
    public static final String RESOURCE_CONTAINS            = "resourceContains";     // search
    public static final String SHARED_RESOURCE_ID           = "sharedResourceId";     // search, sort
    public static final String APPROVER                     = "approver";             // search, sort
    public static final String SHARE_STATUS                 = "shareStatus";          // search, sort
    public static final String PROFILE_NAME                 = "profileName";          // search
    public static final String OWNER_NAME                   = "ownerName";            // search
    public static final String OWNER_TYPE                   = "ownerType";            // search: valid-values(user, group, role)
    public static final String DATA_SHARE_IN_DATASET_ID     = "dataShareInDatasetId"; // search, sort
    public static final String DATASET_IN_PROJECT_ID        = "datasetInProjectId";   // search, sort
    public static final String GDS_PERMISSION               = "gdsPermission";        // search, sort
    public static final String CREATE_TIME_START            = "createdTimeStart";     // search
    public static final String CREATE_TIME_END              = "createdTimeEnd";       // search
    public static final String CREATED_BY                   = "createdBy";            // search
    public static final String UPDATE_TIME_START            = "updatedTimeStart";     // search
    public static final String UPDATE_TIME_END              = "updatedTimeEnd";       // search
    public static final String IS_DISTINCT                  = "isDistinct";           // search, sort
    public static final String RETRIEVE_ALL_PAGES           = "retrieveAllPages";     // search
    public static final String SHARED_WITH_ME               = "sharedWithMe";         // search
    public static final String VALIDITY_EXPIRY_START        = "validityExpiryStart";   // search
    public static final String VALIDITY_EXPIRY_END          = "validityExpiryEnd";    // search
    public static final String VALIDITY_TIME_ZONE           = "timeZone";
    public static final String DEFAULT_TIME_ZONE            = "GMT";
    public static final String DATASET_LABEL_MATCH_TYPE     = "datasetLabelMatchType";
    public static final String DATASET_KEYWORD_MATCH_TYPE   = "datasetKeywordMatchType";

    private Map<String, String>   params;
    private Map<String, Object[]> multiValueParams;
    private int                   startIndex;
    private int                   maxRows    = Integer.MAX_VALUE;
    private boolean               getCount   = true;
    private String                sortBy;
    private String                sortType;
    private boolean               isDistinct = true;

    public SearchFilter() {
        this((Map<String, String>) null);
    }

    public SearchFilter(SearchFilter other) {
        if (other != null) {
            setParams(other.params != null ? new HashMap<>(other.params) : null);
            setMultiValueParams(other.multiValueParams != null ? new HashMap<>(other.multiValueParams) : null);
            setStartIndex(other.startIndex);
            setMaxRows(other.maxRows);
            setGetCount(other.getCount);
            setSortBy(other.sortBy);
            setSortType(other.sortType);
        } else {
            setParams(null);
            setMultiValueParams(null);
        }
    }

    public SearchFilter(String name, String value) {
        setParam(name, value);
    }

    public SearchFilter(Map<String, String> values) {
        setParams(values);
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    public String getParam(String name) {
        return params == null ? null : params.get(name);
    }

    public void setParam(String name, String value) {
        if (StringUtils.isEmpty(name) || StringUtils.isEmpty(value)) {
            return;
        }

        if (params == null) {
            params = new HashMap<>();
        }

        params.put(name, value);
    }

    public void removeParam(String name) {
        params.remove(name);
    }

    public Map<String, String> getParamsWithPrefix(String prefix, boolean stripPrefix) {
        Map<String, String> ret = null;

        if (prefix == null) {
            prefix = StringUtils.EMPTY;
        }

        if (params != null) {
            for (Map.Entry<String, String> e : params.entrySet()) {
                String name = e.getKey();

                if (name.startsWith(prefix)) {
                    if (ret == null) {
                        ret = new HashMap<>();
                    }

                    if (stripPrefix) {
                        name = name.substring(prefix.length());
                    }

                    ret.put(name, e.getValue());
                }
            }
        }

        return ret;
    }

    public boolean isEmpty() {
        return MapUtils.isEmpty(params);
    }

    public Map<String, Object[]> getMultiValueParams() {
        return multiValueParams;
    }

    public void setMultiValueParams(Map<String, Object[]> multiValueParams) {
        this.multiValueParams = multiValueParams;
    }

    public void setMultiValueParam(String name, Object[] value) {
        if (multiValueParams == null) {
            multiValueParams = new HashMap<>();
        }

        multiValueParams.put(name, value);
    }

    public Object[] getMultiValueParam(String name) {
        return multiValueParams != null ? multiValueParams.get(name) : null;
    }

    public int getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(int startIndex) {
        this.startIndex = startIndex;
    }

    public int getMaxRows() {
        return maxRows;
    }

    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    public boolean isGetCount() {
        return getCount;
    }

    public void setGetCount(boolean getCount) {
        this.getCount = getCount;
    }

    public String getSortBy() {
        return sortBy;
    }

    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }

    public String getSortType() {
        return sortType;
    }

    public void setSortType(String sortType) {
        this.sortType = sortType;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    @Override
    public int hashCode() {
        return Objects.hash(params);
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof SearchFilter)) {
            return false;
        }
        SearchFilter that = (SearchFilter) object;
        return Objects.equals(params, that.params);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        toString(sb);

        return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("SearchFilter={");

        sb.append("getCount={").append(getCount).append("} ");
        sb.append("maxRows={").append(maxRows).append("} ");
        sb.append("params={").append(params).append("} ");
        sb.append("sortBy={").append(sortBy).append("} ");
        sb.append("sortType={").append(sortType).append("} ");
        sb.append("startIndex={").append(startIndex).append("} ");
        sb.append("isDistinct={").append(isDistinct).append("} ");
        sb.append("}");

        return sb;
    }
}
