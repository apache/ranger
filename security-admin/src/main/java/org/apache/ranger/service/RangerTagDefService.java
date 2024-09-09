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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.biz.RangerTagDBRetriever;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.SearchField.DATA_TYPE;
import org.apache.ranger.common.SearchField.SEARCH_TYPE;
import org.apache.ranger.entity.XXTagDef;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.util.SearchFilter;
import org.springframework.stereotype.Service;

@Service
public class RangerTagDefService extends RangerTagDefServiceBase<XXTagDef, RangerTagDef> {

	private static final Log logger = LogFactory.getLog(RangerTagDefService.class);

	public RangerTagDefService() {
		searchFields.add(new SearchField(SearchFilter.TAG_DEF_ID, "obj.id", DATA_TYPE.INTEGER, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.TAG_DEF_GUID, "obj.guid", DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.TAG_TYPE, "obj.name", DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.TAG_TYPE_PARTIAL, "obj.name", DATA_TYPE.STRING, SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField(SearchFilter.TAG_SOURCE, "obj.source", DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.TAG_SOURCE_PARTIAL, "obj.source", DATA_TYPE.STRING, SEARCH_TYPE.PARTIAL));

		sortFields.add(new SortField(SearchFilter.TAG_DEF_ID, "obj.id", true, SortField.SORT_ORDER.ASC));
		sortFields.add(new SortField(SearchFilter.TAG_TYPE, "obj.name"));
		sortFields.add(new SortField(SearchFilter.CREATE_TIME,  "obj.createTime"));
		sortFields.add(new SortField(SearchFilter.UPDATE_TIME,  "obj.updateTime"));
	}
	
	@Override
	protected void validateForCreate(RangerTagDef vObj) {

	}

	@Override
	protected void validateForUpdate(RangerTagDef vObj, XXTagDef entityObj) {

	}

	public RangerTagDef getPopulatedViewObject(XXTagDef xObj) {
		return populateViewBean(xObj);
	}

	public RangerTagDef getTagDefByGuid(String guid) {
		RangerTagDef ret = null;

		XXTagDef xxTagDef = daoMgr.getXXTagDef().findByGuid(guid);
		
		if(xxTagDef != null) {
			ret = populateViewBean(xxTagDef);
		}

		return ret;
	}

	public RangerTagDef getTagDefByName(String name) {
		RangerTagDef ret = null;

		XXTagDef xxTagDef = daoMgr.getXXTagDef().findByName(name);
		
		if(xxTagDef != null) {
			ret = populateViewBean(xxTagDef);
		}

		return ret;
	}

	public List<RangerTagDef> getTagDefsByServiceId(Long serviceId) {
		List<RangerTagDef> ret = new ArrayList<RangerTagDef>();

		List<XXTagDef> xxTagDefs = daoMgr.getXXTagDef().findByServiceId(serviceId);
		
		if(CollectionUtils.isNotEmpty(xxTagDefs)) {
			for(XXTagDef xxTagDef : xxTagDefs) {
				RangerTagDef tagDef = populateViewBean(xxTagDef);
				
				ret.add(tagDef);
			}
		}

		return ret;
	}

    @Override
    protected RangerTagDef mapEntityToViewBean(RangerTagDef vObj, XXTagDef xObj) {
        RangerTagDef ret = super.mapEntityToViewBean(vObj, xObj);
		if (StringUtils.isNotEmpty(xObj.getTagAttrDefs())) {
			try {
				List<RangerTagDef.RangerTagAttributeDef> attributeDefs = (List<RangerTagDef.RangerTagAttributeDef>) JsonUtils.jsonToObject(xObj.getTagAttrDefs(), RangerTagDBRetriever.subsumedDataType);
				ret.setAttributeDefs(attributeDefs);
			} catch (JsonProcessingException e) {
				logger.error("Error occurred while processing json", e);
			}
		}
        return ret;
    }

    @Override
    protected XXTagDef mapViewToEntityBean(RangerTagDef vObj, XXTagDef xObj, int OPERATION_CONTEXT) {
        XXTagDef ret = super.mapViewToEntityBean(vObj, xObj, OPERATION_CONTEXT);
        ret.setTagAttrDefs(JsonUtils.listToJson(vObj.getAttributeDefs()));
        return ret;
    }

    @Override
    public List<RangerTagDef.RangerTagAttributeDef> getAttributeDefForTagDef(XXTagDef xtagDef) {
        return new ArrayList<>();
    }

}
