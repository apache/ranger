package org.apache.ranger.service;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.SearchField.DATA_TYPE;
import org.apache.ranger.common.SearchField.SEARCH_TYPE;
import org.apache.ranger.common.view.VTrxLogAttr;
import org.apache.ranger.db.XXAccessTypeDefDao;
import org.apache.ranger.db.XXPolicyResourceDao;
import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPolicyConditionDef;
import org.apache.ranger.entity.XXPolicyItem;
import org.apache.ranger.entity.XXPolicyItemAccess;
import org.apache.ranger.entity.XXPolicyItemCondition;
import org.apache.ranger.entity.XXPolicyResource;
import org.apache.ranger.entity.XXPolicyResourceMap;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.util.SearchFilter;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class RangerPolicyService extends RangerPolicyServiceBase<XXPolicy, RangerPolicy> {

	@Autowired
	JSONUtil jsonUtil;
	
	public static final String POLICY_RESOURCE_CLASS_FIELD_NAME = "resources";
	public static final String POLICY_ITEM_CLASS_FIELD_NAME = "policyItems";

	static HashMap<String, VTrxLogAttr> trxLogAttrs = new HashMap<String, VTrxLogAttr>();
	String actionCreate;
	String actionUpdate;
	String actionDelete;

	static {
		trxLogAttrs.put("name", new VTrxLogAttr("name", "Policy Name", false));
		trxLogAttrs.put("description", new VTrxLogAttr("description", "Policy Description", false));
		trxLogAttrs.put("isEnabled", new VTrxLogAttr("isEnabled", "Policy Status", false));
		trxLogAttrs.put("resources", new VTrxLogAttr("resources", "Policy Resources", false));
		trxLogAttrs.put("policyItems", new VTrxLogAttr("policyItems", "Policy Items", false));
	}
	
	public RangerPolicyService() {
		super();
		actionCreate = "create";
		actionUpdate = "update";
		actionDelete = "delete";
		
		searchFields.add(new SearchField(SearchFilter.SERVICE_TYPE, "xSvcDef.name", DATA_TYPE.STRING, 
				SEARCH_TYPE.FULL, "XXServiceDef xSvcDef, XXService xSvc", "xSvc.type = xSvcDef.id and xSvc.id = obj.service"));
		searchFields.add(new SearchField(SearchFilter.SERVICE_TYPE_ID, "xSvc.type", DATA_TYPE.INTEGER, SEARCH_TYPE.FULL, 
				"XXService xSvc", "xSvc.id = obj.service"));
		searchFields.add(new SearchField(SearchFilter.SERVICE_NAME, "xSvc.name", DATA_TYPE.STRING, SEARCH_TYPE.FULL, 
				"XXService xSvc", "xSvc.id = obj.service"));
		searchFields.add(new SearchField(SearchFilter.SERVICE_ID, "xSvc.id", DATA_TYPE.INTEGER, SEARCH_TYPE.FULL, 
				"XXService xSvc", "xSvc.id = obj.service"));
		searchFields.add(new SearchField(SearchFilter.STATUS, "obj.isEnabled", DATA_TYPE.BOOLEAN, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.POLICY_ID, "obj.id", DATA_TYPE.INTEGER, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.POLICY_NAME, "obj.name", DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.USER, "xUser.name", DATA_TYPE.STRING, SEARCH_TYPE.FULL, 
				"XXUser xUser, XXPolicyItem xPolItem, XXPolicyItemUserPerm userPerm", "obj.id = xPolItem.policyId "
						+ "and userPerm.policyItemId = xPolItem.id and xUser.id = userPerm.userId"));
		searchFields.add(new SearchField(SearchFilter.GROUP, "xGrp.name", DATA_TYPE.STRING, SEARCH_TYPE.FULL, 
				"XXGroup xGrp, XXPolicyItem xPolItem, XXPolicyItemGroupPerm grpPerm", "obj.id = xPolItem.policyId "
						+ "and grpPerm.policyItemId = xPolItem.id and xGrp.id = grpPerm.groupId"));
		
		sortFields.add(new SortField(SearchFilter.CREATE_TIME, "obj.createTime"));
		sortFields.add(new SortField(SearchFilter.UPDATE_TIME, "obj.updateTime"));
		sortFields.add(new SortField(SearchFilter.POLICY_ID, "obj.id"));
		sortFields.add(new SortField(SearchFilter.POLICY_NAME, "obj.name"));
	}
	
	@Override
	protected void validateForCreate(RangerPolicy vObj) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void validateForUpdate(RangerPolicy vObj, XXPolicy entityObj) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	protected RangerPolicy populateViewBean(XXPolicy xPolicy) {
		RangerPolicy vPolicy = super.populateViewBean(xPolicy);
		
		Map<String, RangerPolicyResource> resources = getResourcesForXXPolicy(xPolicy);
		vPolicy.setResources(resources);
		
		List<RangerPolicyItem> policyItems = getPolicyItemListForXXPolicy(xPolicy);
		vPolicy.setPolicyItems(policyItems);
		
		return vPolicy;
	}
	
	public List<RangerPolicyItem> getPolicyItemListForXXPolicy(XXPolicy xPolicy) {
		
		List<RangerPolicyItem> policyItems = new ArrayList<RangerPolicyItem>();
		List<XXPolicyItem> xPolicyItemList = daoMgr.getXXPolicyItem().findByPolicyId(xPolicy.getId());
		
		for(XXPolicyItem xPolItem : xPolicyItemList) {
			RangerPolicyItem policyItem = populateXXToRangerPolicyItem(xPolItem);
			policyItems.add(policyItem);
		}
		return policyItems;
	}

	public RangerPolicyItem populateXXToRangerPolicyItem(XXPolicyItem xPolItem) {
		
		RangerPolicyItem rangerPolItem = new RangerPolicyItem();
		
		List<XXPolicyItemAccess> xPolItemAccList = daoMgr
				.getXXPolicyItemAccess().findByPolicyItemId(xPolItem.getId());
		List<RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicyItemAccess>();
		
		XXAccessTypeDefDao xAccDefDao = daoMgr.getXXAccessTypeDef();
		for(XXPolicyItemAccess xPolAccess : xPolItemAccList) {
			RangerPolicyItemAccess access = new RangerPolicyItemAccess();
			access.setIsAllowed(xPolAccess.getIsallowed());
			XXAccessTypeDef xAccessType = xAccDefDao.getById(xPolAccess.getType());
			access.setType(xAccessType.getName());
			
			accesses.add(access);
		}
		rangerPolItem.setAccesses(accesses);
		
		List<RangerPolicyItemCondition> conditions = new ArrayList<RangerPolicyItemCondition>();
		List<XXPolicyConditionDef> xConditionDefList = daoMgr
				.getXXPolicyConditionDef()
				.findByPolicyItemId(xPolItem.getId());
		for(XXPolicyConditionDef xCondDef : xConditionDefList) {
			
			List<XXPolicyItemCondition> xPolCondItemList = daoMgr
					.getXXPolicyItemCondition().findByPolicyItemAndDefId(
							xPolItem.getId(), xCondDef.getId());
			List<String> values = new ArrayList<String>();
			
			for(XXPolicyItemCondition polCond : xPolCondItemList) {
				values.add(polCond.getValue());
			}
			
			RangerPolicyItemCondition condition = new RangerPolicyItemCondition();
			condition.setType(xCondDef.getName());
			condition.setValues(values);
			
			conditions.add(condition);
		}
		rangerPolItem.setConditions(conditions);
		
		List<String> userList = daoMgr.getXXUser().findByPolicyItemId(xPolItem.getId());
		List<String> grpList = daoMgr.getXXGroup().findByPolicyItemId(xPolItem.getId());
		
		rangerPolItem.setUsers(userList);
		rangerPolItem.setGroups(grpList);
		
		rangerPolItem.setDelegateAdmin(xPolItem.getDelegateAdmin());
		return rangerPolItem;
	}

	public Map<String, RangerPolicyResource> getResourcesForXXPolicy(XXPolicy xPolicy) {
		List<XXResourceDef> resDefList = daoMgr.getXXResourceDef().findByPolicyId(xPolicy.getId());
		Map<String, RangerPolicyResource> resources = new HashMap<String, RangerPolicyResource>();
		
		XXPolicyResourceDao xPolResDao = daoMgr.getXXPolicyResource();
		for(XXResourceDef xResDef : resDefList) {
			XXPolicyResource xPolRes = xPolResDao.findByResDefIdAndPolicyId(
					xResDef.getId(), xPolicy.getId());
			if(xPolRes == null) {
				continue;
			}
			List<String> values = new ArrayList<>();
			List<XXPolicyResourceMap> xPolResMapList = daoMgr
					.getXXPolicyResourceMap()
					.findByPolicyResId(xPolRes.getId());
			for(XXPolicyResourceMap xPolResMap : xPolResMapList) {
				values.add(xPolResMap.getValue());
			}
			RangerPolicyResource resource = new RangerPolicyResource();
			resource.setValues(values);
			resource.setIsExcludes(xPolRes.getIsexcludes());
			resource.setIsRecursive(xPolRes.getIsrecursive());
			
			resources.put(xResDef.getName(), resource);
		}
		return resources;
	}
	
	public RangerPolicy getPopulatedViewObject(XXPolicy xPolicy) {
		return this.populateViewBean(xPolicy);
	}
	
	public List<XXTrxLog> getTransactionLog(RangerPolicy vPolicy, int action){
		return getTransactionLog(vPolicy, null, action);
	}

	public List<XXTrxLog> getTransactionLog(RangerPolicy vObj, XXPolicy mObj, int action) {
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
				XXTrxLog xTrxLog = processFieldToCreateTrxLog(field, objectName, nameField, vObj, mObj, action);
				if (xTrxLog != null) {
					trxLogList.add(xTrxLog);
				}
			}

			Field[] superClassFields = vObj.getClass().getSuperclass()
					.getDeclaredFields();
			for (Field field : superClassFields) {
				if (field.getName().equalsIgnoreCase("isEnabled")) {
					XXTrxLog xTrx = processFieldToCreateTrxLog(field, objectName, nameField, vObj, mObj, action);
					if (xTrx != null) {
						trxLogList.add(xTrx);
					}
					break;
				}
			}
		} catch (IllegalAccessException illegalAcc) {
			illegalAcc.printStackTrace();
		} catch (NoSuchFieldException noSuchField) {
			noSuchField.printStackTrace();
		}
		
		return trxLogList;
	}
	
	private XXTrxLog processFieldToCreateTrxLog(Field field, String objectName,
			Field nameField, RangerPolicy vObj, XXPolicy mObj, int action) {

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

			} else if (fieldName.equalsIgnoreCase(POLICY_RESOURCE_CLASS_FIELD_NAME)) {
				value = processPolicyResourcesForTrxLog(field.get(vObj));
			} else if (fieldName.equalsIgnoreCase(POLICY_ITEM_CLASS_FIELD_NAME)) {
				value = processPolicyItemsForTrxLog(field.get(vObj));
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
				RangerPolicy oldPolicy = populateViewBean(mObj);
				if (fieldName.equalsIgnoreCase(POLICY_RESOURCE_CLASS_FIELD_NAME)) {
					oldValue = processPolicyResourcesForTrxLog(oldPolicy.getResources());
				} else if (fieldName.equalsIgnoreCase(POLICY_ITEM_CLASS_FIELD_NAME)) {
					oldValue = processPolicyItemsForTrxLog(oldPolicy.getPolicyItems());
				}
				if (value.equalsIgnoreCase(oldValue)) {
					return null;
				} else if (fieldName.equalsIgnoreCase(POLICY_RESOURCE_CLASS_FIELD_NAME)) {
					// Compare old and new resources
					if(compareTwoPolicyResources(value, oldValue)) {
						return null;
					}
				} else if (fieldName.equalsIgnoreCase(POLICY_ITEM_CLASS_FIELD_NAME)) {
					//Compare old and new policyItems
					if(compareTwoPolicyItemList(value, oldValue)) {
						return null;
					}
				}
				xTrxLog.setPreviousValue(oldValue);
				xTrxLog.setNewValue(value);
			}
		} catch (IllegalArgumentException | IllegalAccessException e) {
			e.printStackTrace();
		}

		xTrxLog.setAction(actionString);
		xTrxLog.setObjectClassType(AppConstants.CLASS_TYPE_RANGER_POLICY);
		xTrxLog.setObjectId(vObj.getId());
		xTrxLog.setObjectName(objectName);

		return xTrxLog;
	}

	private boolean compareTwoPolicyItemList(String value, String oldValue) {
		if (value == null && oldValue == null) {
			return true;
		}
		if (value == "" && oldValue == "") {
			return true;
		}
		if (stringUtil.isEmpty(value) || stringUtil.isEmpty(oldValue)) {
			return false;
		}

		ObjectMapper mapper = new ObjectMapper();
		try {
			List<RangerPolicyItem> obj = mapper.readValue(value,
					new TypeReference<List<RangerPolicyItem>>() {
					});
			List<RangerPolicyItem> oldObj = mapper.readValue(oldValue,
					new TypeReference<List<RangerPolicyItem>>() {
					});
			
			int oldListSize = oldObj.size();
			int listSize = obj.size();
			if(oldListSize != listSize) {
				return false;
			}
			
			for(RangerPolicyItem polItem : obj) {
				if(!oldObj.contains(polItem)) {
					return false;
				}
			}
			return true;
		} catch (JsonParseException e) {
			throw restErrorUtil.createRESTException(
					"Invalid input data: " + e.getMessage(),
					MessageEnums.INVALID_INPUT_DATA);
		} catch (JsonMappingException e) {
			throw restErrorUtil.createRESTException(
					"Invalid input data: " + e.getMessage(),
					MessageEnums.INVALID_INPUT_DATA);
		} catch (IOException e) {
			throw restErrorUtil.createRESTException(
					"Invalid input data: " + e.getMessage(),
					MessageEnums.INVALID_INPUT_DATA);
		}
	}

	private boolean compareTwoPolicyResources(String value, String oldValue) {
		if (value == null && oldValue == null) {
			return true;
		}
		if (value == "" && oldValue == "") {
			return true;
		}
		if (stringUtil.isEmpty(value) || stringUtil.isEmpty(oldValue)) {
			return false;
		}

		ObjectMapper mapper = new ObjectMapper();
		try {
			Map<String, RangerPolicyResource> obj = mapper.readValue(value,
					new TypeReference<Map<String, RangerPolicyResource>>() {
					});
			Map<String, RangerPolicyResource> oldObj = mapper.readValue(oldValue,
					new TypeReference<Map<String, RangerPolicyResource>>() {
					});
			
			if (obj.size() != oldObj.size()) {
				return false;
			}
			
			for (String key : obj.keySet()) {
				if (!obj.get(key).equals(oldObj.get(key))) {
					return false;
				}
			}
			return true;
		} catch (JsonParseException e) {
			throw restErrorUtil.createRESTException(
					"Invalid input data: " + e.getMessage(),
					MessageEnums.INVALID_INPUT_DATA);
		} catch (JsonMappingException e) {
			throw restErrorUtil.createRESTException(
					"Invalid input data: " + e.getMessage(),
					MessageEnums.INVALID_INPUT_DATA);
		} catch (IOException e) {
			throw restErrorUtil.createRESTException(
					"Invalid input data: " + e.getMessage(),
					MessageEnums.INVALID_INPUT_DATA);
		}
	}

	@SuppressWarnings("unchecked")
	private String processPolicyItemsForTrxLog(Object value) {
		if(value == null) {
			return "";
		}
		List<RangerPolicyItem> rangerPolicyItems = (List<RangerPolicyItem>) value;
		String ret = jsonUtil.readListToString(rangerPolicyItems);
		if(ret == null) {
			return "";
		}
		return ret;
	}

	@SuppressWarnings("unchecked")
	private String processPolicyResourcesForTrxLog(Object value) {
		if (value == null) {
			return "";
		}
		Map<String, RangerPolicyResource> resources = (Map<String, RangerPolicyResource>) value;
		String ret = jsonUtil.readMapToString(resources);
		if(ret == null) {
			return "";
		}
		return ret;
	}

}
