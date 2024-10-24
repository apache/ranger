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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.view.VXGroup;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

@Service
@Scope("singleton")
public class XGroupService extends XGroupServiceBase<XXGroup, VXGroup> {

	private final Long createdByUserId;

	public XGroupService() {
		searchFields.add(new SearchField("name", "obj.name",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField("groupSource", "obj.groupSource",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));

		searchFields.add(new SearchField("isVisible", "obj.isVisible",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL ));
		
		searchFields.add(new SearchField("userId", "groupUser.userId",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL,
				"XXGroupUser groupUser", "obj.id = groupUser.parentGroupId"));

		searchFields.add(new SearchField("syncSource", "obj.syncSource",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));

		createdByUserId = PropertiesUtil.getLongProperty("ranger.xuser.createdByUserId", 1);

		sortFields.add(new SortField("name", "obj.name",true,SortField.SORT_ORDER.ASC));
	}

	@Override
	protected void validateForCreate(VXGroup vObj) {
		XXGroup xxGroup = daoManager.getXXGroup().findByGroupName(
				vObj.getName());
		if (xxGroup != null) {
			throw restErrorUtil.createRESTException("XGroup already exists",
					MessageEnums.ERROR_DUPLICATE_OBJECT);
		}

	}

	@Override
	protected void validateForUpdate(VXGroup vObj, XXGroup mObj) {
		if (!vObj.getName().equalsIgnoreCase(mObj.getName())) {
			validateForCreate(vObj);
		}
	}

	public VXGroup getGroupByGroupName(String groupName) {
		XXGroup xxGroup = daoManager.getXXGroup().findByGroupName(groupName);

		if (xxGroup == null) {
			throw restErrorUtil.createRESTException(
					groupName + " is Not Found", MessageEnums.DATA_NOT_FOUND);
		}
		return super.populateViewBean(xxGroup);
	}
	
	public VXGroup createXGroupWithOutLogin(VXGroup vxGroup) {
		XXGroup xxGroup = daoManager.getXXGroup().findByGroupName(
				vxGroup.getName());
		boolean groupExists = true;

		if (xxGroup == null) {
			xxGroup = new XXGroup();
			groupExists = false;
		}

		xxGroup = mapViewToEntityBean(vxGroup, xxGroup, 0);
		XXPortalUser xXPortalUser = daoManager.getXXPortalUser().getById(createdByUserId);
		if (xXPortalUser != null) {
			xxGroup.setAddedByUserId(createdByUserId);
			xxGroup.setUpdatedByUserId(createdByUserId);
		}
		if (groupExists) {
			getDao().update(xxGroup);
		} else {
			getDao().create(xxGroup);
		}
		xxGroup = daoManager.getXXGroup().findByGroupName(vxGroup.getName());
		vxGroup = postCreate(xxGroup);
		return vxGroup;
	}

	public VXGroup readResourceWithOutLogin(Long id) {
		XXGroup resource = getDao().getById(id);
		if (resource == null) {
			// Returns code 400 with DATA_NOT_FOUND as the error message
			throw restErrorUtil.createRESTException(getResourceName()
					+ " not found", MessageEnums.DATA_NOT_FOUND, id, null,
					"preRead: " + id + " not found.");
		}

		VXGroup view = populateViewBean(resource);
		if(view!=null){
			view.setGroupSource(resource.getGroupSource());
		}
		return view;
	}

	@Override
	public VXGroup populateViewBean(XXGroup xGroup) {
		VXGroup vObj = super.populateViewBean(xGroup);
		vObj.setIsVisible(xGroup.getIsVisible());
		return vObj;
	}
	
	@Override
	protected XXGroup mapViewToEntityBean(VXGroup vObj, XXGroup mObj, int OPERATION_CONTEXT) {
		return super.mapViewToEntityBean(vObj, mObj, OPERATION_CONTEXT);
	}

	@Override
	protected VXGroup mapEntityToViewBean(VXGroup vObj, XXGroup mObj) {
		return super.mapEntityToViewBean(vObj, mObj);
        }

        public Map<Long, XXGroup> getXXGroupIdXXGroupMap(){
                Map<Long, XXGroup> xXGroupMap=new HashMap<Long, XXGroup>();
                try{
                        List<XXGroup> xXGroupList=daoManager.getXXGroup().getAll();
                        if(!CollectionUtils.isEmpty(xXGroupList)){
                                for(XXGroup xXGroup:xXGroupList){
                                        xXGroupMap.put(xXGroup.getId(), xXGroup);
                                }
                        }
                }catch(Exception ex){}
                return xXGroupMap;
        }

	public Map<Long, String> getXXGroupIdNameMap() {
		return daoManager.getXXGroup().getAllGroupIdNames();
	}

	public Long getAllGroupCount() {
		return daoManager.getXXGroup().getAllCount();
	}

	public List<XXGroup> getGroupsByUserId(Long userId) {
		return daoManager.getXXGroup().findByUserId(userId);
	}
}
