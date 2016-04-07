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

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SearchField.DATA_TYPE;
import org.apache.ranger.common.SearchField.SEARCH_TYPE;
import org.apache.ranger.entity.XXTag;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.util.SearchFilter;
import org.springframework.stereotype.Service;


@Service
public class RangerTagService extends RangerTagServiceBase<XXTag, RangerTag> {

	public RangerTagService() {
		searchFields.add(new SearchField(SearchFilter.TAG_ID, "obj.id", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.TAG_DEF_ID, "obj.type", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.TAG_TYPE, "tagDef.name", DATA_TYPE.STRING, SEARCH_TYPE.FULL, "XXTagDef tagDef", "obj.type = tagDef.id"));
	}

	@Override
	protected void validateForCreate(RangerTag vObj) {

	}

	@Override
	protected void validateForUpdate(RangerTag vObj, XXTag entityObj) {

	}

	@Override
	public RangerTag postUpdate(XXTag tag) {
		RangerTag ret = super.postUpdate(tag);

		daoMgr.getXXServiceVersionInfo().updateServiceVersionInfoForTagUpdate(tag.getId(), tag.getUpdateTime());

		return ret;
	}

	public RangerTag getPopulatedViewObject(XXTag xObj) {
		return populateViewBean(xObj);
	}

	public RangerTag getTagByGuid(String guid) {
		RangerTag ret = null;

		XXTag xxTag = daoMgr.getXXTag().findByGuid(guid);
		
		if(xxTag != null) {
			ret = populateViewBean(xxTag);
		}

		return ret;
	}

	public List<RangerTag> getTagsByType(String name) {
		List<RangerTag> ret = new ArrayList<RangerTag>();

		List<XXTag> xxTags = daoMgr.getXXTag().findByName(name);
		
		if(CollectionUtils.isNotEmpty(xxTags)) {
			for(XXTag xxTag : xxTags) {
				RangerTag tag = populateViewBean(xxTag);

				ret.add(tag);
			}
		}

		return ret;
	}

	public List<RangerTag> getTagsForResourceId(Long resourceId) {
		List<RangerTag> ret = new ArrayList<RangerTag>();

		List<XXTag> xxTags = daoMgr.getXXTag().findForResourceId(resourceId);
		
		if(CollectionUtils.isNotEmpty(xxTags)) {
			for(XXTag xxTag : xxTags) {
				RangerTag tag = populateViewBean(xxTag);

				ret.add(tag);
			}
		}

		return ret;
	}

	public List<RangerTag> getTagsForResourceGuid(String resourceGuid) {
		List<RangerTag> ret = new ArrayList<RangerTag>();

		List<XXTag> xxTags = daoMgr.getXXTag().findForResourceGuid(resourceGuid);
		
		if(CollectionUtils.isNotEmpty(xxTags)) {
			for(XXTag xxTag : xxTags) {
				RangerTag tag = populateViewBean(xxTag);

				ret.add(tag);
			}
		}

		return ret;
	}

	public List<RangerTag> getTagsByServiceId(Long serviceId) {
		List<RangerTag> ret = new ArrayList<RangerTag>();

		List<XXTag> xxTags = daoMgr.getXXTag().findByServiceId(serviceId);
		
		if(CollectionUtils.isNotEmpty(xxTags)) {
			for(XXTag xxTag : xxTags) {
				RangerTag tag = populateViewBean(xxTag);

				ret.add(tag);
			}
		}

		return ret;
	}
}