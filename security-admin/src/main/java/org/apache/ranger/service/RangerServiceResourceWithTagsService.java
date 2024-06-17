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

package org.apache.ranger.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.SearchField.DATA_TYPE;
import org.apache.ranger.common.SearchField.SEARCH_TYPE;
import org.apache.ranger.entity.XXServiceResource;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceResourceWithTags;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.view.RangerServiceResourceWithTagsList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class RangerServiceResourceWithTagsService extends RangerServiceResourceWithTagsServiceBase<XXServiceResource, RangerServiceResourceWithTags> {

	private static final Logger LOG = LoggerFactory.getLogger(RangerServiceResourceWithTagsService.class);

	public RangerServiceResourceWithTagsService() {
        searchFields.add(new SearchField(SearchFilter.TAG_RESOURCE_ID,          "obj.id",                      DATA_TYPE.INTEGER,  SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.TAG_SERVICE_ID,           "obj.serviceId",               DATA_TYPE.INTEGER,  SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.TAG_SERVICE_NAME,         "service.name",                DATA_TYPE.STRING,   SEARCH_TYPE.FULL,    "XXService service", "obj.serviceId = service.id"));
        searchFields.add(new SearchField(SearchFilter.TAG_SERVICE_NAME_PARTIAL, "service.name",                DATA_TYPE.STRING,   SEARCH_TYPE.PARTIAL, "XXService service", "obj.serviceId = service.id"));
        searchFields.add(new SearchField(SearchFilter.TAG_RESOURCE_GUID,        "obj.guid",                    DATA_TYPE.STRING,   SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.TAG_RESOURCE_SIGNATURE,   "obj.resourceSignature",       DATA_TYPE.STRING,   SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.TAG_RESOURCE_IDS,         "obj.id",                      DATA_TYPE.INT_LIST, SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.TAG_RESOURCE_ELEMENTS,    "obj.serviceResourceElements", DATA_TYPE.STRING,   SEARCH_TYPE.PARTIAL));
        searchFields.add(new SearchField(SearchFilter.TAG_NAMES,                "tagDef.name",                 DATA_TYPE.STR_LIST, SEARCH_TYPE.FULL,    "XXTagResourceMap map, XXTag tag, XXTagDef tagDef", "obj.id = map.resourceId and map.tagId = tag.id and tag.type = tagDef.id"));

        sortFields.add(new SortField(SearchFilter.TAG_RESOURCE_ID, "obj.id", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField(SearchFilter.TAG_SERVICE_ID,  "obj.serviceId"));
        sortFields.add(new SortField(SearchFilter.CREATE_TIME,     "obj.createTime"));
        sortFields.add(new SortField(SearchFilter.UPDATE_TIME,     "obj.updateTime"));
    }

	@Override
	protected XXServiceResource mapViewToEntityBean(RangerServiceResourceWithTags viewBean, XXServiceResource t, int OPERATION_CONTEXT) {
		return null;
	}

	@Override
	protected void validateForCreate(RangerServiceResourceWithTags vObj) {
	}

	@Override
	protected void validateForUpdate(RangerServiceResourceWithTags vObj, XXServiceResource entityObj) {
	}

	public RangerServiceResourceWithTags getPopulatedViewObject(XXServiceResource xObj) {
		return this.populateViewBean(xObj);
	}

	public RangerServiceResourceWithTagsList searchServiceResourcesWithTags(SearchFilter filter) {
		LOG.debug("==> searchServiceResourcesWithTags({})", filter);

		RangerServiceResourceWithTagsList   ret          = new RangerServiceResourceWithTagsList();
		List<XXServiceResource>             xObjList     = super.searchResources(filter, searchFields, sortFields, ret);
		List<RangerServiceResourceWithTags> resourceList = new ArrayList<>();

		if (xObjList != null) {
			for (XXServiceResource resource:xObjList) {
				resourceList.add(getPopulatedViewObject(resource));
			}
		}

		ret.setResourceList(resourceList);

		LOG.debug("<== searchServiceResourcesWithTags({}): ret={}", filter, ret);

		return ret;
	}

	@Override
    protected RangerServiceResourceWithTags mapEntityToViewBean(RangerServiceResourceWithTags serviceResourceWithTags, XXServiceResource xxServiceResource) {
		RangerServiceResourceWithTags ret = super.mapEntityToViewBean(serviceResourceWithTags, xxServiceResource);

        if (StringUtils.isNotEmpty(xxServiceResource.getServiceResourceElements())) {
			try {
				Map<String, RangerPolicyResource> serviceResourceElements = (Map<String, RangerPolicyResource>) JsonUtils.jsonToObject(xxServiceResource.getServiceResourceElements(), RangerServiceResourceService.subsumedDataType);

				if (MapUtils.isNotEmpty(serviceResourceElements)) {
					ret.setResourceElements(serviceResourceElements);
				} else {
					LOG.info("Empty serviceResourceElement in [" + ret + "]!!");
				}
			} catch (JsonProcessingException e) {
				LOG.error("Error occurred while processing json", e);
			}
        } else {
            LOG.info("Empty string representing serviceResourceElements in [" + xxServiceResource + "]!!");
        }

        return ret;
    }
}
