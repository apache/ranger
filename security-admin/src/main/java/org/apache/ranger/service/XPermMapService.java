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

import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.entity.XXPermMap;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.view.VXGroup;
import org.apache.ranger.view.VXPermMap;
import org.apache.ranger.view.VXUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class XPermMapService extends XPermMapServiceBase<XXPermMap, VXPermMap> {

	@Autowired
	XGroupService xGroupService;
	
	@Autowired
	XUserService xUserService;

	
	public XPermMapService() {
		searchFields.add(new SearchField("resourceId", "obj.resourceId",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("permType", "obj.permType",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("permFor", "obj.permFor",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("userId", "obj.userId",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("groupId", "obj.groupId",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
	}

	@Override
	protected void validateForCreate(VXPermMap vObj) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void validateForUpdate(VXPermMap vObj, XXPermMap mObj) {
		// TODO Auto-generated method stub

	}
	
	@Override
	public VXPermMap populateViewBean(XXPermMap xXPermMap){
		VXPermMap map = super.populateViewBean(xXPermMap);		
		if(map.getPermFor() == AppConstants.XA_PERM_FOR_GROUP) {
			String groupName = getGroupName(map.getGroupId());
			if(groupName != null){
				map.setGroupName(groupName);
			}
		} else if(map.getPermFor() == AppConstants.XA_PERM_FOR_USER) {
			String username = getUserName(map.getUserId());
			if(username != null){
				map.setUserName(username);
			}
		}
		return map;
	}
	
	public String getGroupName(Long groupId){
		if(groupId!=null && groupId!=0){
		VXGroup vXGroup = xGroupService.readResource(groupId);
			return vXGroup.getName();
		}
		else
			return null;
	}
	
	public String getUserName(Long userId){
		if(userId!=null && userId!=0){
		VXUser vXUser = xUserService.readResource(userId);
			return vXUser.getName();
		}
		else
			return null;
	}

	@Override
	protected XXPermMap mapViewToEntityBean(VXPermMap vObj, XXPermMap mObj, int OPERATION_CONTEXT) {
	    XXPermMap ret = null;
		if(vObj!=null && mObj!=null){
			ret = super.mapViewToEntityBean(vObj, mObj, OPERATION_CONTEXT);
			XXPortalUser xXPortalUser=null;
			if(ret.getAddedByUserId()==null || ret.getAddedByUserId()==0){
				if(!stringUtil.isEmpty(vObj.getOwner())){
					xXPortalUser=daoManager.getXXPortalUser().findByLoginId(vObj.getOwner());
					if(xXPortalUser!=null){
						ret.setAddedByUserId(xXPortalUser.getId());
					}
				}
			}
			if(ret.getUpdatedByUserId()==null || ret.getUpdatedByUserId()==0){
				if(!stringUtil.isEmpty(vObj.getUpdatedBy())){
					xXPortalUser= daoManager.getXXPortalUser().findByLoginId(vObj.getUpdatedBy());
					if(xXPortalUser!=null){
						ret.setUpdatedByUserId(xXPortalUser.getId());
					}
				}
			}
		}
		return ret;
	}

	@Override
	protected VXPermMap mapEntityToViewBean(VXPermMap vObj, XXPermMap mObj) {
	    VXPermMap ret = null;
		if(mObj!=null && vObj!=null){
            ret = super.mapEntityToViewBean(vObj, mObj);
            XXPortalUser xXPortalUser=null;
			if(stringUtil.isEmpty(ret.getOwner())){
				xXPortalUser= daoManager.getXXPortalUser().getById(mObj.getAddedByUserId());
				if(xXPortalUser!=null){
					ret.setOwner(xXPortalUser.getLoginId());
				}
			}
			if(stringUtil.isEmpty(ret.getUpdatedBy())){
				xXPortalUser= daoManager.getXXPortalUser().getById(mObj.getUpdatedByUserId());
				if(xXPortalUser!=null){
					ret.setUpdatedBy(xXPortalUser.getLoginId());
				}
			}
		}
		return ret;
	}
}
