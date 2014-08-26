package com.xasecure.service;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.xasecure.common.SearchField;

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
public class XAuditMapService extends
		XAuditMapServiceBase<XXAuditMap, VXAuditMap> {

	@Autowired
	XAEnumUtil xaEnumUtil;

	@Autowired
	XADaoManager xADaoManager;

	static HashMap<String, VTrxLogAttr> trxLogAttrs = new HashMap<String, VTrxLogAttr>();
	static {
//		trxLogAttrs.put("groupId", new VTrxLogAttr("groupId", "Group Audit", false));
//		trxLogAttrs.put("userId", new VTrxLogAttr("userId", "User Audit", false));
		trxLogAttrs.put("auditType", new VTrxLogAttr("auditType", "Audit Type", true));
	}

	public XAuditMapService() {
		searchFields.add(new SearchField("resourceId", "obj.resourceId",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
	}

	@Override
	protected void validateForCreate(VXAuditMap vObj) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void validateForUpdate(VXAuditMap vObj, XXAuditMap mObj) {
		// TODO Auto-generated method stub

	}

	public List<XXTrxLog> getTransactionLog(VXAuditMap vXAuditMap, String action){
		return getTransactionLog(vXAuditMap, null, action);
	}

	public List<XXTrxLog> getTransactionLog(VXAuditMap vObj, VXAuditMap mObj, String action){
		if(vObj == null && (action == null || !action.equalsIgnoreCase("update"))){
			return null;
		}
		
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
					String enumName = XXAuditMap.getEnumName(fieldName);
					int enumValue = field.get(vObj) == null ? 0 : Integer.parseInt(""+field.get(vObj));
					value = xaEnumUtil.getLabel(enumName, enumValue);
				} else {
					value = ""+field.get(vObj);
					XXUser xUser = xADaoManager.getXXUser().getById(Long.parseLong(value));
					value = xUser.getName();
				}
				
				if(action.equalsIgnoreCase("create")){
					xTrxLog.setNewValue(value);
				} else if(action.equalsIgnoreCase("delete")){
					xTrxLog.setPreviousValue(value);
				} else if(action.equalsIgnoreCase("update")){
					// Not Changed.
					xTrxLog.setNewValue(value);
					xTrxLog.setPreviousValue(value);
				}
				
				xTrxLog.setAction(action);
				xTrxLog.setObjectClassType(AppConstants.CLASS_TYPE_XA_AUDIT_MAP);
				xTrxLog.setObjectId(vObj.getId());
				xTrxLog.setParentObjectClassType(AppConstants.CLASS_TYPE_XA_RESOURCE);
				xTrxLog.setParentObjectId(vObj.getResourceId());
//				xTrxLog.setParentObjectName(vObj.get);
//				xTrxLog.setObjectName(objectName);
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

	@Override
	protected XXAuditMap mapViewToEntityBean(VXAuditMap vObj, XXAuditMap mObj, int OPERATION_CONTEXT) {
		super.mapViewToEntityBean(vObj, mObj, OPERATION_CONTEXT);
		if(vObj!=null && mObj!=null){
			XXPortalUser xXPortalUser=null;
			if(mObj.getAddedByUserId()==null || mObj.getAddedByUserId()==0){
				if(!stringUtil.isEmpty(vObj.getOwner())){
					xXPortalUser=xADaoManager.getXXPortalUser().findByLoginId(vObj.getOwner());	
					if(xXPortalUser!=null){
						mObj.setAddedByUserId(xXPortalUser.getId());
					}
				}
			}
			if(mObj.getUpdatedByUserId()==null || mObj.getUpdatedByUserId()==0){
				if(!stringUtil.isEmpty(vObj.getUpdatedBy())){
					xXPortalUser= xADaoManager.getXXPortalUser().findByLoginId(vObj.getUpdatedBy());			
					if(xXPortalUser!=null){
						mObj.setUpdatedByUserId(xXPortalUser.getId());
					}		
				}
			}
		}
		return mObj;
	}

	@Override
	protected VXAuditMap mapEntityToViewBean(VXAuditMap vObj, XXAuditMap mObj) {
		super.mapEntityToViewBean(vObj, mObj);
		if(mObj!=null && vObj!=null){
			XXPortalUser xXPortalUser=null;
			if(stringUtil.isEmpty(vObj.getOwner())){
				xXPortalUser= xADaoManager.getXXPortalUser().getById(mObj.getAddedByUserId());	
				if(xXPortalUser!=null){
					vObj.setOwner(xXPortalUser.getLoginId());
				}
			}
			if(stringUtil.isEmpty(vObj.getUpdatedBy())){
				xXPortalUser= xADaoManager.getXXPortalUser().getById(mObj.getUpdatedByUserId());		
				if(xXPortalUser!=null){
					vObj.setUpdatedBy(xXPortalUser.getLoginId());
				}
			}
		}
		return vObj;
	}
}
