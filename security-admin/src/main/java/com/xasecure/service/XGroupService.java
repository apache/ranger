package com.xasecure.service;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.xasecure.common.XACommonEnums;
import com.xasecure.common.MessageEnums;
import com.xasecure.common.PropertiesUtil;
import com.xasecure.common.SearchField;
import com.xasecure.common.SortField;
import com.xasecure.common.SortField.SORT_ORDER;
import com.xasecure.common.StringUtil;
import com.xasecure.entity.*;
import com.xasecure.view.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.xasecure.biz.*;
import com.xasecure.common.AppConstants;
import com.xasecure.common.view.VTrxLogAttr;
import com.xasecure.db.XADaoManager;
import com.xasecure.entity.*;
import com.xasecure.service.*;
import com.xasecure.util.XAEnumUtil;
import com.xasecure.view.*;

@Service
@Scope("singleton")
public class XGroupService extends XGroupServiceBase<XXGroup, VXGroup> {

	public static Long createdByUserId = 1L;
	
	@Autowired
	XADaoManager xADaoManager;
	
	@Autowired
	XAEnumUtil xaEnumUtil;
	
	@Autowired
	StringUtil stringUtil;
	
	static HashMap<String, VTrxLogAttr> trxLogAttrs = new HashMap<String, VTrxLogAttr>();
	static {
		trxLogAttrs.put("name", new VTrxLogAttr("name", "Group Name", false));
		trxLogAttrs.put("description", new VTrxLogAttr("description", "Group Description", false));
	}
	
	public XGroupService() {
		searchFields.add(new SearchField("name", "obj.name",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField("groupSource", "obj.groupSource",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		 createdByUserId = new Long(PropertiesUtil.getIntProperty(
				"xa.xuser.createdByUserId", 1));		 
		
		 sortFields.add(new SortField("name", "obj.name",true,SortField.SORT_ORDER.ASC));
	}

	@Override
	protected void validateForCreate(VXGroup vObj) {
		XXGroup xxGroup = xADaoManager.getXXGroup().findByGroupName(
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
		XXGroup xxGroup = xADaoManager.getXXGroup().findByGroupName(groupName);

		if (xxGroup == null) {
			throw restErrorUtil.createRESTException(
					groupName + " is Not Found", MessageEnums.DATA_NOT_FOUND);
		}
		return super.populateViewBean(xxGroup);
	}
	
	public VXGroup createXGroupWithOutLogin(VXGroup vxGroup) {
		XXGroup xxGroup = xADaoManager.getXXGroup().findByGroupName(
				vxGroup.getName());
		boolean groupExists = true;

		if (xxGroup == null) {
			xxGroup = new XXGroup();
			groupExists = false;
		}

		xxGroup = mapViewToEntityBean(vxGroup, xxGroup, 0);
		XXPortalUser xXPortalUser = xADaoManager.getXXPortalUser().getById(createdByUserId);
		if (xXPortalUser != null) {
			xxGroup.setAddedByUserId(createdByUserId);
			xxGroup.setUpdatedByUserId(createdByUserId);
		}
		if (groupExists) {
			xxGroup = getDao().update(xxGroup);
		} else {
			xxGroup = getDao().create(xxGroup);
		}
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

	public List<XXTrxLog> getTransactionLog(VXGroup vResource, String action){
		return getTransactionLog(vResource, null, action);
	}

	public List<XXTrxLog> getTransactionLog(VXGroup vObj, XXGroup mObj, String action){
		if(vObj == null && (action == null || !action.equalsIgnoreCase("update"))){
			return null;
		}
		
		List<XXTrxLog> trxLogList = new ArrayList<XXTrxLog>();
		try {

			Field nameField = vObj.getClass().getDeclaredField("name");
			nameField.setAccessible(true);
			String objectName = ""+nameField.get(vObj);
			Field[] fields = vObj.getClass().getDeclaredFields();
			
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
					String enumName = XXGroup.getEnumName(fieldName);
					int enumValue = field.get(vObj) == null ? 0 : Integer.parseInt(""+field.get(vObj));
					value = xaEnumUtil.getLabel(enumName, enumValue);
				} else {
					value = ""+field.get(vObj);
				}
				
				if(action.equalsIgnoreCase("create")){
					if(stringUtil.isEmpty(value)){
						continue;
					}
					xTrxLog.setNewValue(value);
				} else if(action.equalsIgnoreCase("delete")){
					xTrxLog.setPreviousValue(value);
				} else if(action.equalsIgnoreCase("update")){
					String oldValue = null;
					Field[] mFields = mObj.getClass().getDeclaredFields();
					for(Field mField : mFields){
						mField.setAccessible(true);
						String mFieldName = mField.getName();
						if(fieldName.equalsIgnoreCase(mFieldName)){
							if(isEnum){
								String enumName = XXAsset.getEnumName(mFieldName);
								int enumValue = mField.get(mObj) == null ? 0 : Integer.parseInt(""+mField.get(mObj));
								oldValue = xaEnumUtil.getLabel(enumName, enumValue);
							} else {
								oldValue = mField.get(mObj)+"";
							}
							break;
						}
					}
					if(value.equalsIgnoreCase(oldValue)){
						continue;
					}
					xTrxLog.setPreviousValue(oldValue);
					xTrxLog.setNewValue(value);
				}
				
				xTrxLog.setAction(action);
				xTrxLog.setObjectClassType(AppConstants.CLASS_TYPE_XA_GROUP);
				xTrxLog.setObjectId(vObj.getId());
				xTrxLog.setObjectName(objectName);
				trxLogList.add(xTrxLog);
				
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		}
		
		return trxLogList;
	}
	
	@Override
	protected XXGroup mapViewToEntityBean(VXGroup vObj, XXGroup mObj, int OPERATION_CONTEXT) {
		super.mapViewToEntityBean(vObj, mObj, OPERATION_CONTEXT);
		return mObj;
	}

	@Override
	protected VXGroup mapEntityToViewBean(VXGroup vObj, XXGroup mObj) {
		super.mapEntityToViewBean(vObj, mObj);
		return vObj;
	}	
}
