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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.view.VTrxLogAttr;
import org.apache.ranger.entity.XXAsset;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXGroupUser;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.util.RangerEnumUtil;
import org.apache.ranger.view.VXGroupUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

@Service
@Scope("singleton")
public class XGroupUserService extends
		XGroupUserServiceBase<XXGroupUser, VXGroupUser> {

	private final Long createdByUserId;

	@Autowired
	RangerEnumUtil xaEnumUtil;

	@Autowired
	@Qualifier(value = "transactionManager")
	PlatformTransactionManager txManager;
	
	static HashMap<String, VTrxLogAttr> trxLogAttrs = new HashMap<String, VTrxLogAttr>();
	static {
		trxLogAttrs.put("parentGroupId", new VTrxLogAttr("parentGroupId", "Group Name", false));
	}


	public XGroupUserService() {
		searchFields.add(new SearchField("xUserId", "obj.userId",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("xGroupId", "obj.parentGroupId",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		createdByUserId = PropertiesUtil.getLongProperty("ranger.xuser.createdByUserId", 1);
		
	}

	@Override
	protected void validateForCreate(VXGroupUser vObj) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void validateForUpdate(VXGroupUser vObj, XXGroupUser mObj) {
		// TODO Auto-generated method stub

	}
	
	public VXGroupUser createXGroupUserWithOutLogin(VXGroupUser vxGroupUser) {
		boolean groupUserMappingExists = true;
		XXGroupUser xxGroupUser = daoManager.getXXGroupUser().findByGroupNameAndUserId(vxGroupUser.getName(), vxGroupUser.getUserId());
		if (xxGroupUser == null) {
			xxGroupUser = new XXGroupUser();
			groupUserMappingExists = false;
		}

		XXGroup xGroup = daoManager.getXXGroup().findByGroupName(vxGroupUser.getName());
		vxGroupUser.setParentGroupId(xGroup.getId());
		xxGroupUser = mapViewToEntityBean(vxGroupUser, xxGroupUser, 0);
		XXPortalUser xXPortalUser = daoManager.getXXPortalUser().getById(createdByUserId);
		if (xXPortalUser != null) {
			xxGroupUser.setAddedByUserId(createdByUserId);
			xxGroupUser.setUpdatedByUserId(createdByUserId);
		}
		if (groupUserMappingExists) {
			xxGroupUser = getDao().update(xxGroupUser);
		} else {
			xxGroupUser = getDao().create(xxGroupUser);
		}
		vxGroupUser = postCreate(xxGroupUser);
		return vxGroupUser;
	}

	public void createOrUpdateXGroupUsers(String groupName, Set<String> users, Map<String, Long> usersFromDB) {
		XXGroup xxGroup = daoManager.getXXGroup().findByGroupName(groupName);
		if (logger.isDebugEnabled()) {
			logger.debug("createOrUpdateXGroupUsers(): groupname =  " + groupName + " users = " + users);
		}
		if (xxGroup == null) {
			if (logger.isDebugEnabled()) {
				logger.debug("createOrUpdateXGroupUsers(): groupname =  " + groupName
						+ " doesn't exist in database. Hence ignoring group membership updates");
			}
			return;
		}
		Map<String, XXGroupUser> groupUsers = daoManager.getXXGroupUser().findUsersByGroupName(groupName);
		for (String username : users) {
			if (usersFromDB.containsKey(username)) {
				// Add or update group user mapping only if the user exists in x_user table.
				TransactionTemplate txTemplate = new TransactionTemplate(txManager);
				txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
				try {
					txTemplate.execute(new TransactionCallback<Object>() {
						@Override
						public Object doInTransaction(TransactionStatus status) {
							XXGroupUser xxGroupUser = groupUsers.get(username);
							boolean groupUserMappingExists = true;
							if (xxGroupUser == null) {
								xxGroupUser = new XXGroupUser();
								groupUserMappingExists = false;
							}
							xxGroupUser.setAddedByUserId(createdByUserId);
							xxGroupUser.setUpdatedByUserId(createdByUserId);

							if (groupUserMappingExists) {
								xxGroupUser = getDao().update(xxGroupUser);
							} else {
								VXGroupUser vXGroupUser = new VXGroupUser();
								vXGroupUser.setUserId(usersFromDB.get(username));
								vXGroupUser.setName(groupName);
								vXGroupUser.setParentGroupId(xxGroup.getId());
								xxGroupUser = mapViewToEntityBean(vXGroupUser, xxGroupUser, 0);
								xxGroupUser = getDao().create(xxGroupUser);
							}
							VXGroupUser vXGroupUser = postCreate(xxGroupUser);
							if (logger.isDebugEnabled()) {
								logger.debug(String.format("createOrUpdateXGroupUsers(): Create or update group user mapping with groupname =  " + vXGroupUser.getName()
										+ " username = %s userId = %d", username, vXGroupUser.getUserId()));
							}
							return null;
						}
					});
				} catch (Throwable ex) {
					logger.error("XGroupUserService.createOrUpdateXGroupUsers: Failed to update DB for group users: ", ex);
					throw restErrorUtil.createRESTException("Failed to create or update group users ",
							MessageEnums.ERROR_CREATING_OBJECT);
				}
			}
		}
	}

	public VXGroupUser readResourceWithOutLogin(Long id) {
		XXGroupUser resource = getDao().getById(id);
		if (resource == null) {
			// Returns code 400 with DATA_NOT_FOUND as the error message
			throw restErrorUtil.createRESTException(getResourceName()
							+ " not found", MessageEnums.DATA_NOT_FOUND, id, null,
					"preRead: " + id + " not found.");
		}

		VXGroupUser view = populateViewBean(resource);
		return view;
	}
	
	public List<XXTrxLog> getTransactionLog(VXGroupUser vXGroupUser, String action){
		return getTransactionLog(vXGroupUser, null, action);
	}

	public List<XXTrxLog> getTransactionLog(VXGroupUser vObj, XXGroupUser mObj, String action){
//		if(vObj == null && (action == null || !action.equalsIgnoreCase("update"))){
//			return null;
//		}
		
		Long groupId = vObj.getParentGroupId();
		XXGroup xGroup = daoManager.getXXGroup().getById(groupId);
		String groupName = xGroup.getName();

		Long userId = vObj.getUserId();
		XXUser xUser = daoManager.getXXUser().getById(userId);
		String userName = xUser.getName();

		List<XXTrxLog> trxLogList = new ArrayList<XXTrxLog>();
		Field[] fields = vObj.getClass().getDeclaredFields();
		
		try {
			for(Field field : fields){
				field.setAccessible(true);
				String fieldName = field.getName();
				if(!trxLogAttrs.containsKey(fieldName)){
					continue;
				}
				
				VTrxLogAttr vTrxLogAttr = trxLogAttrs.get(fieldName);
				
				XXTrxLog xTrxLog = new XXTrxLog();
				xTrxLog.setAttributeName(vTrxLogAttr.getAttribUserFriendlyName());
			
				String value = null;
				boolean isEnum = vTrxLogAttr.isEnum();
				if(isEnum){
					String enumName = XXAsset.getEnumName(fieldName);
					int enumValue = field.get(vObj) == null ? 0 : Integer.parseInt(""+field.get(vObj));
					value = xaEnumUtil.getLabel(enumName, enumValue);
				} else {
					value = ""+field.get(vObj);
					XXGroup xXGroup = daoManager.getXXGroup().getById(Long.parseLong(value));
					value = xXGroup.getName();
				}
				
				if("create".equalsIgnoreCase(action)){
					xTrxLog.setNewValue(value);
				} else if("delete".equalsIgnoreCase(action)){
					xTrxLog.setPreviousValue(value);
				} else if("update".equalsIgnoreCase(action)){
					// No Change.
					xTrxLog.setNewValue(value);
					xTrxLog.setPreviousValue(value);
				}
				
				xTrxLog.setAction(action);
				
				xTrxLog.setObjectClassType(AppConstants.CLASS_TYPE_XA_GROUP_USER);
				xTrxLog.setObjectId(vObj.getId());
				xTrxLog.setObjectName(userName);
				
				xTrxLog.setParentObjectClassType(AppConstants.CLASS_TYPE_XA_GROUP);
				xTrxLog.setParentObjectId(groupId);
				xTrxLog.setParentObjectName(groupName);
				
				trxLogList.add(xTrxLog);
				
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		}
		
		return trxLogList;
	}

	
}
