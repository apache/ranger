package org.apache.ranger.service;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SearchField.DATA_TYPE;
import org.apache.ranger.common.SearchField.SEARCH_TYPE;
import org.apache.ranger.common.SearchUtil;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.view.VTrxLogAttr;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceConfigMap;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.util.SearchFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class RangerServiceService extends RangerServiceServiceBase<XXService, RangerService> {

	@Autowired
	JSONUtil jsonUtil;

	private String hiddenPasswordString;

	static HashMap<String, VTrxLogAttr> trxLogAttrs = new HashMap<String, VTrxLogAttr>();
	String actionCreate;
	String actionUpdate;
	String actionDelete;
	static {
		trxLogAttrs.put("name", new VTrxLogAttr("name", "Service Name", false));
		trxLogAttrs.put("description", new VTrxLogAttr("description", "Service Description", false));
		trxLogAttrs.put("isEnabled", new VTrxLogAttr("isEnabled", "Service Status", false));
		trxLogAttrs.put("configs", new VTrxLogAttr("configs", "Connection Configurations", false));
		trxLogAttrs.put("policyVersion", new VTrxLogAttr("policyVersion", "Policy Version", false));
		trxLogAttrs.put("policyUpdateTime", new VTrxLogAttr("policyUpdateTime", "Policy Update Time", false));
	}
	
	public RangerServiceService() {
		super();
		hiddenPasswordString = PropertiesUtil.getProperty("xa.password.hidden", "*****");
		actionCreate = "create";
		actionUpdate = "update";
		actionDelete = "delete";
		
		searchFields.add(new SearchField(SearchFilter.SERVICE_TYPE, "xSvcDef.name", DATA_TYPE.STRING, 
				SEARCH_TYPE.FULL, "XXServiceDef xSvcDef", "obj.type = xSvcDef.id"));
		searchFields.add(new SearchField(SearchFilter.SERVICE_TYPE_ID, "obj.type", DATA_TYPE.INTEGER, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.SERVICE_NAME, "obj.name", DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.SERVICE_ID, "obj.id", DATA_TYPE.INTEGER, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.STATUS, "obj.isEnabled", DATA_TYPE.BOOLEAN, SEARCH_TYPE.FULL));
		
		sortFields.add(new SortField(SearchFilter.CREATE_TIME, "obj.createTime"));
		sortFields.add(new SortField(SearchFilter.UPDATE_TIME, "obj.updateTime"));
		sortFields.add(new SortField(SearchFilter.SERVICE_ID, "obj.id"));
		sortFields.add(new SortField(SearchFilter.SERVICE_NAME, "obj.name"));
	}
	
	@Override
	protected void validateForCreate(RangerService vObj) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void validateForUpdate(RangerService vService, XXService xService) {
		
	}
	
	@Override
	protected RangerService populateViewBean(XXService xService) {
		RangerService vService = super.populateViewBean(xService);
		
		HashMap<String, String> configs = new HashMap<String, String>();
		List<XXServiceConfigMap> svcConfigMapList = daoMgr.getXXServiceConfigMap()
				.findByServiceId(xService.getId());
		for(XXServiceConfigMap svcConfMap : svcConfigMapList) {
			configs.put(svcConfMap.getConfigkey(), svcConfMap.getConfigvalue());
		}
		vService.setConfigs(configs);
		
		return vService;
	}
	
	public RangerService getPopulatedViewObject(XXService xService) {
		return this.populateViewBean(xService);
	}
	
	public List<RangerService> getAllServices() {
		List<XXService> xxServiceList = daoMgr.getXXService().getAll();
		List<RangerService> serviceList = new ArrayList<RangerService>();
		
		for(XXService xxService : xxServiceList) {
			RangerService service = populateViewBean(xxService);
			serviceList.add(service);
		}
		return serviceList;
	}
	
	public List<XXTrxLog> getTransactionLog(RangerService vService, int action){
		return getTransactionLog(vService, null, action);
	}

	public List<XXTrxLog> getTransactionLog(RangerService vObj, XXService mObj, int action) {
		if (vObj == null && (action == 0 || action != OPERATION_UPDATE_CONTEXT)) {
			return null;
		}
		List<XXTrxLog> trxLogList = new ArrayList<XXTrxLog>();
		Field[] fields = vObj.getClass().getDeclaredFields();

		try {
			Field nameField = vObj.getClass().getDeclaredField("name");
			nameField.setAccessible(true);
			String objectName = "" + nameField.get(vObj);

			for (Field field : fields) {
				if (!trxLogAttrs.containsKey(field.getName())) {
					continue;
				}
				XXTrxLog xTrxLog = processFieldToCreateTrxLog(field,
						objectName, nameField, vObj, mObj, action);
				if (xTrxLog != null) {
					trxLogList.add(xTrxLog);
				}
			}
			Field[] superClassFields = vObj.getClass().getSuperclass().getDeclaredFields();
			for(Field field : superClassFields) {
				if(field.getName().equalsIgnoreCase("isEnabled")) {
					XXTrxLog xTrx = processFieldToCreateTrxLog(field, objectName, nameField, vObj, mObj, action);
					if(xTrx != null) {
						trxLogList.add(xTrx);
					}
					break;
				}
			}
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		}
		return trxLogList;
	}

	@SuppressWarnings("unchecked")
	private XXTrxLog processFieldToCreateTrxLog(Field field, String objectName,
			Field nameField, RangerService vObj, XXService mObj, int action) {

		String actionString = "";

		field.setAccessible(true);
		String fieldName = field.getName();
		XXTrxLog xTrxLog = new XXTrxLog();

		try {
			VTrxLogAttr vTrxLogAttr = trxLogAttrs.get(fieldName);

			xTrxLog.setAttributeName(vTrxLogAttr.getAttribUserFriendlyName());

			String value = null;
			boolean isEnum = vTrxLogAttr.isEnum();
			if (isEnum) {

			} else if (fieldName.equalsIgnoreCase("configs")) {
				Map<String, String> configs = (field.get(vObj) != null) ? (Map<String, String>) field
						.get(vObj) : new HashMap<String, String>();

				value = jsonUtil.readMapToString(configs);
			} else {
				value = "" + field.get(vObj);
			}

			if (action == OPERATION_CREATE_CONTEXT) {
				if (stringUtil.isEmpty(value)) {
					return null;
				}
				xTrxLog.setNewValue(value);
				actionString = actionCreate;
			} else if (action == OPERATION_DELETE_CONTEXT) {
				xTrxLog.setPreviousValue(value);
				actionString = actionDelete;
			} else if (action == OPERATION_UPDATE_CONTEXT) {
				actionString = actionUpdate;
				String oldValue = null;
				Field[] mFields = mObj.getClass().getDeclaredFields();
				for (Field mField : mFields) {
					mField.setAccessible(true);
					String mFieldName = mField.getName();
					if (fieldName.equalsIgnoreCase(mFieldName)) {
						if (isEnum) {

						} else {
							oldValue = mField.get(mObj) + "";
						}
						break;
					}
				}
				if (fieldName.equalsIgnoreCase("configs")) {
					Map<String, String> vConfig = jsonUtil.jsonToMap(value);
					RangerService oldService = this.populateViewBean(mObj);
					Map<String, String> xConfig = oldService.getConfigs();

					Map<String, String> newConfig = new HashMap<String, String>();
					Map<String, String> oldConfig = new HashMap<String, String>();

					for (Entry<String, String> entry : vConfig.entrySet()) {

						String key = entry.getKey();
						if (!xConfig.containsKey(key)) {
							newConfig.put(key, entry.getValue());
						} else if (!entry.getValue().equalsIgnoreCase(
								xConfig.get(key))) {
							if (key.equalsIgnoreCase("password")
									&& entry.getValue().equalsIgnoreCase(
											hiddenPasswordString)) {
								continue;
							}
							newConfig.put(key, entry.getValue());
							oldConfig.put(key, xConfig.get(key));
						}
					}
					oldValue = jsonUtil.readMapToString(oldConfig);
					value = jsonUtil.readMapToString(newConfig);
				}
				if (value.equalsIgnoreCase(oldValue)) {
					return null;
				}
				xTrxLog.setPreviousValue(oldValue);
				xTrxLog.setNewValue(value);
			}
		} catch (IllegalArgumentException | IllegalAccessException e) {
			e.printStackTrace();
		}

		xTrxLog.setAction(actionString);
		xTrxLog.setObjectClassType(AppConstants.CLASS_TYPE_XA_SERVICE);
		xTrxLog.setObjectId(vObj.getId());
		xTrxLog.setObjectName(objectName);

		return xTrxLog;
	}

}
