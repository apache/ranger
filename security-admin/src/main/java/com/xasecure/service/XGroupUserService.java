package com.xasecure.service;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.xasecure.common.XACommonEnums;
import com.xasecure.common.MessageEnums;
import com.xasecure.common.PropertiesUtil;
import com.xasecure.common.SearchField;
import com.xasecure.entity.XXPortalUser;
import com.xasecure.entity.XXGroup;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.xasecure.common.AppConstants;
import com.xasecure.common.view.VTrxLogAttr;
import com.xasecure.db.XADaoManager;
import com.xasecure.entity.*;
import com.xasecure.util.XAEnumUtil;
import com.xasecure.view.*;

@Service
@Scope("singleton")
public class XGroupUserService extends
		XGroupUserServiceBase<XXGroupUser, VXGroupUser> {

	public static Long createdByUserId = 1L;
	
	@Autowired
	XADaoManager xADaoManager;

	@Autowired
	XAEnumUtil xaEnumUtil;
	
	static HashMap<String, VTrxLogAttr> trxLogAttrs = new HashMap<String, VTrxLogAttr>();
	static {
		trxLogAttrs.put("parentGroupId", new VTrxLogAttr("parentGroupId", "Group Name", false));
	}


	public XGroupUserService() {
		searchFields.add(new SearchField("xUserId", "obj.userId",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("xGroupId", "obj.parentGroupId",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		createdByUserId = new Long(PropertiesUtil.getIntProperty(
				"xa.xuser.createdByUserId", 1));
		
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
		XXGroupUser xxGroupUser = new XXGroupUser();
		XXGroup xGroup = daoManager.getXXGroup().findByGroupName(vxGroupUser.getName());
		vxGroupUser.setParentGroupId(xGroup.getId());
		xxGroupUser = mapViewToEntityBean(vxGroupUser, xxGroupUser, 0);
		XXPortalUser xXPortalUser = xADaoManager.getXXPortalUser().getById(createdByUserId);
		if (xXPortalUser != null) {
			xxGroupUser.setAddedByUserId(createdByUserId);
			xxGroupUser.setUpdatedByUserId(createdByUserId);
		}
		xxGroupUser = getDao().create(xxGroupUser);
		vxGroupUser = postCreate(xxGroupUser);
		return vxGroupUser;
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
		XXGroup xGroup = xADaoManager.getXXGroup().getById(groupId);
		String groupName = xGroup.getName();

		Long userId = vObj.getUserId();
		XXUser xUser = xADaoManager.getXXUser().getById(userId);
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
					XXGroup xXGroup = xADaoManager.getXXGroup().getById(Long.parseLong(value));
					value = xXGroup.getName();
				}
				
				if(action.equalsIgnoreCase("create")){
					xTrxLog.setNewValue(value);
				} else if(action.equalsIgnoreCase("delete")){
					xTrxLog.setPreviousValue(value);
				} else if(action.equalsIgnoreCase("update")){
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
