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

import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceResource;
import org.apache.ranger.plugin.model.RangerServiceResourceWithTags;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.SearchFilter;

public abstract class RangerServiceResourceWithTagsServiceBase<T extends XXServiceResource, V extends RangerServiceResourceWithTags> extends RangerBaseModelService<T, V> {

	@Override
	protected V mapEntityToViewBean(V vObj, T xObj) {
		XXService xService = daoMgr.getXXService().getById(xObj.getServiceId());

		vObj.setGuid(xObj.getGuid());
		vObj.setVersion(xObj.getVersion());
		vObj.setIsEnabled(xObj.getIsEnabled());
		vObj.setServiceName(xService.getName());
		vObj.setAssociatedTags(JsonUtils.jsonToRangerTagList(xObj.getTags()));

		return vObj;
	}

	public PList<V> searchServiceResources(SearchFilter searchFilter) {
		PList<V> retList       = new PList<V>();
		List<V>  resourceList  = new ArrayList<V>();
		List<T>  xResourceList = searchRangerObjects(searchFilter, searchFields, sortFields, retList);

		for (T xResource : xResourceList) {
			V taggedRes = populateViewBean(xResource);

			resourceList.add(taggedRes);
		}

		retList.setList(resourceList);
		retList.setResultSize(resourceList.size());
		retList.setPageSize(searchFilter.getMaxRows());
		retList.setStartIndex(searchFilter.getStartIndex());
		retList.setSortType(searchFilter.getSortType());
		retList.setSortBy(searchFilter.getSortBy());

		return retList;
	}
}
