/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.service;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.biz.RangerPolicyRetriever;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.view.VTrxLogAttr;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPolicyBase;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicy.RangerRowFilterPolicyItem;
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
	public static final String POLICY_NAME_CLASS_FIELD_NAME = "name";
	public static final String POLICY_DESCRIPTION_CLASS_FIELD_NAME = "description";
	public static final String DENYPOLICY_ITEM_CLASS_FIELD_NAME = "denyPolicyItems";
	public static final String ALLOW_EXCEPTIONS_CLASS_FIELD_NAME="allowExceptions";
	public static final String DENY_EXCEPTIONS_CLASS_FIELD_NAME="denyExceptions";
	public static final String DATAMASK_POLICY_ITEM_CLASS_FIELD_NAME="dataMaskPolicyItems";
	public static final String ROWFILTER_POLICY_ITEM_CLASS_FIELD_NAME="rowFilterPolicyItems";
	public static final String IS_ENABLED_CLASS_FIELD_NAME="isEnabled";
	public static final String IS_AUDIT_ENABLED_CLASS_FIELD_NAME="isAuditEnabled";

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
		trxLogAttrs.put("denyPolicyItems", new VTrxLogAttr("denyPolicyItems", "DenyPolicy Items", false));
		trxLogAttrs.put("allowExceptions", new VTrxLogAttr("allowExceptions", "Allow Exceptions", false));
		trxLogAttrs.put("denyExceptions", new VTrxLogAttr("denyExceptions", "Deny Exceptions", false));
		trxLogAttrs.put("dataMaskPolicyItems", new VTrxLogAttr("dataMaskPolicyItems", "Masked Policy Items", false));
		trxLogAttrs.put("rowFilterPolicyItems", new VTrxLogAttr("rowFilterPolicyItems", "Row level filter Policy Items", false));
		trxLogAttrs.put("isAuditEnabled", new VTrxLogAttr("isAuditEnabled", "Audit Status", false));
	}
	
	public RangerPolicyService() {
		super();
		actionCreate = "create";
		actionUpdate = "update";
		actionDelete = "delete";
	}

	@Override
	protected XXPolicy mapViewToEntityBean(RangerPolicy vObj, XXPolicy xObj, int OPERATION_CONTEXT) {
		return (XXPolicy)super.mapViewToEntityBean(vObj, (XXPolicyBase)xObj, OPERATION_CONTEXT);
	}

	@Override
	protected RangerPolicy mapEntityToViewBean(RangerPolicy vObj, XXPolicy xObj) {
		return super.mapEntityToViewBean(vObj, (XXPolicyBase)xObj);
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
		RangerPolicyRetriever retriever = new RangerPolicyRetriever(daoMgr);

		RangerPolicy vPolicy = retriever.getPolicy(xPolicy);
		
		return vPolicy;
	}
	
	public RangerPolicy getPopulatedViewObject(XXPolicy xPolicy) {
		return this.populateViewBean(xPolicy);
	}
	
	public List<XXTrxLog> getTransactionLog(RangerPolicy vPolicy, int action){
		return getTransactionLog(vPolicy, null, action);
	}

	public List<XXTrxLog> getTransactionLog(RangerPolicy vObj, XXPolicy mObj, int action) {
		if (vObj == null || action == 0 || (action == OPERATION_UPDATE_CONTEXT && mObj == null)) {
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
			} else if (fieldName.equalsIgnoreCase(DENYPOLICY_ITEM_CLASS_FIELD_NAME)) {
				value = processPolicyItemsForTrxLog(field.get(vObj));
			} else if (fieldName.equalsIgnoreCase(POLICY_NAME_CLASS_FIELD_NAME)){
				value = processPolicyNameForTrxLog(field.get(vObj));
			} else if (fieldName.equalsIgnoreCase(ALLOW_EXCEPTIONS_CLASS_FIELD_NAME)){
				value = processPolicyItemsForTrxLog(field.get(vObj));
			} else if (fieldName.equalsIgnoreCase(DENY_EXCEPTIONS_CLASS_FIELD_NAME)){
				value = processPolicyItemsForTrxLog(field.get(vObj));
			} else if (fieldName.equalsIgnoreCase(DATAMASK_POLICY_ITEM_CLASS_FIELD_NAME)){
				value = processDataMaskPolicyItemsForTrxLog(field.get(vObj));
			} else if (fieldName.equalsIgnoreCase(ROWFILTER_POLICY_ITEM_CLASS_FIELD_NAME)){
				value = processRowFilterPolicyItemForTrxLog(field.get(vObj));
			} else if (fieldName.equalsIgnoreCase(IS_ENABLED_CLASS_FIELD_NAME)){
				value = String.valueOf(processIsEnabledClassFieldNameForTrxLog(field.get(vObj)));
			
			}
			else {
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
					if (oldPolicy != null) {
						oldValue = processPolicyResourcesForTrxLog(oldPolicy.getResources());
					}
				} else if (fieldName.equalsIgnoreCase(POLICY_ITEM_CLASS_FIELD_NAME)) {
					if (oldPolicy != null) {
						oldValue = processPolicyItemsForTrxLog(oldPolicy.getPolicyItems());
					}
				} else if (fieldName.equalsIgnoreCase(DENYPOLICY_ITEM_CLASS_FIELD_NAME)) {
					if (oldPolicy != null) {
						oldValue = processPolicyItemsForTrxLog(oldPolicy.getDenyPolicyItems());
					}
				} else if (fieldName.equalsIgnoreCase(POLICY_NAME_CLASS_FIELD_NAME)){
					if (oldPolicy != null) {
						oldValue = processPolicyNameForTrxLog(oldPolicy.getName());
					}
				} else if (fieldName.equalsIgnoreCase(POLICY_DESCRIPTION_CLASS_FIELD_NAME)){
					if (oldPolicy != null) {
						oldValue = processPolicyNameForTrxLog(oldPolicy.getDescription());
					}
				}  else if (fieldName.equalsIgnoreCase(ALLOW_EXCEPTIONS_CLASS_FIELD_NAME)) {
					if (oldPolicy != null) {
						oldValue = processPolicyItemsForTrxLog(oldPolicy.getAllowExceptions());
					}
				} else if (fieldName.equalsIgnoreCase(DENY_EXCEPTIONS_CLASS_FIELD_NAME)) {
					if (oldPolicy != null) {
						oldValue = processPolicyItemsForTrxLog(oldPolicy.getDenyExceptions());
					}
				} else if (fieldName.equalsIgnoreCase(DATAMASK_POLICY_ITEM_CLASS_FIELD_NAME)) {
					if (oldPolicy != null) {
						oldValue = processDataMaskPolicyItemsForTrxLog(oldPolicy.getDataMaskPolicyItems());
					}
				} else if (fieldName.equalsIgnoreCase(ROWFILTER_POLICY_ITEM_CLASS_FIELD_NAME)) {
					if (oldPolicy != null) {
						oldValue = processRowFilterPolicyItemForTrxLog(oldPolicy.getRowFilterPolicyItems());
					}
				}else if (fieldName.equalsIgnoreCase(IS_ENABLED_CLASS_FIELD_NAME)) {
					if (oldPolicy != null) {
						oldValue = String.valueOf(processIsEnabledClassFieldNameForTrxLog(oldPolicy.getIsEnabled()));
					}
				}
				if (oldValue == null || oldValue.equalsIgnoreCase(value)) {
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
				} else if (fieldName.equalsIgnoreCase(POLICY_NAME_CLASS_FIELD_NAME)) {
					//compare old and new policyName
					if(compareTwoPolicyName(value, oldValue)) {
						return null;
					}
				} else if (fieldName.equalsIgnoreCase(DENYPOLICY_ITEM_CLASS_FIELD_NAME)) {
					//compare old and new denyPolicyItem
					if(compareTwoPolicyItemList(value, oldValue)) {
						return null;
					}
				} else if (fieldName.equalsIgnoreCase(ALLOW_EXCEPTIONS_CLASS_FIELD_NAME)) {
					//compare old and new allowExceptions
					if(compareTwoPolicyItemList(value, oldValue)) {
						return null;
					}
				} else if (fieldName.equalsIgnoreCase(DENY_EXCEPTIONS_CLASS_FIELD_NAME)) {
					//compare old and new denyExceptions
					if(compareTwoPolicyItemList(value, oldValue)) {
						return null;
					}
				} else if (fieldName.equalsIgnoreCase(POLICY_DESCRIPTION_CLASS_FIELD_NAME)) {
					//compare old and new Description
					if(org.apache.commons.lang.StringUtils.equals(value, oldValue)) {
						return null;
					}
				} else if (fieldName.equalsIgnoreCase(DATAMASK_POLICY_ITEM_CLASS_FIELD_NAME)) {
					//compare old and new dataMaskPolicyItems
					if(compareTwoDataMaskingPolicyItemList(value, oldValue)) {
						return null;
					}
				} else if (fieldName.equalsIgnoreCase(ROWFILTER_POLICY_ITEM_CLASS_FIELD_NAME)) {
					//compare old and new rowFilterPolicyItems
					if(compareTwoRowFilterPolicyItemList(value, oldValue)) {
						return null;
					}
				} else if (fieldName.equalsIgnoreCase(IS_ENABLED_CLASS_FIELD_NAME)) {
					if (oldPolicy != null) {
					    oldValue=processPolicyNameForTrxLog(String.valueOf(oldPolicy.getIsEnabled()));
					}
				} else if (fieldName.equalsIgnoreCase(IS_AUDIT_ENABLED_CLASS_FIELD_NAME)) {
					if (oldPolicy != null) {
					    oldValue=processPolicyNameForTrxLog(String.valueOf(oldPolicy.getIsAuditEnabled()));
					}
				} else if (fieldName.equalsIgnoreCase(IS_ENABLED_CLASS_FIELD_NAME)) {
					if(compareTwoPolicyName(value, oldValue)) {
					    return null;
					}
				} else if (fieldName.equalsIgnoreCase(IS_AUDIT_ENABLED_CLASS_FIELD_NAME)) {
					if(compareTwoPolicyName(value, oldValue)) {
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
		
		XXService parentObj = daoMgr.getXXService().findByName(vObj.getService());
		xTrxLog.setParentObjectClassType(AppConstants.CLASS_TYPE_XA_SERVICE);
		xTrxLog.setParentObjectId(parentObj.getId());
		xTrxLog.setParentObjectName(parentObj.getName());

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
			
			for (Map.Entry<String, RangerPolicyResource> entry : obj.entrySet()) {
				if (!entry.getValue().equals(oldObj.get(entry.getKey()))) {
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
		if(rangerPolicyItems==null || rangerPolicyItems.size()==0){
			return "";
		}
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

	private boolean compareTwoPolicyName(String value, String oldValue) {
		return org.apache.commons.lang.StringUtils.equals(value, oldValue);
	}

	private String processPolicyNameForTrxLog(Object value) {
		if (value == null) {
			return "";
		}
		String name = (String) value;
		return name;
	}

	@SuppressWarnings("unchecked")
	private String processDataMaskPolicyItemsForTrxLog(Object value) {
		if(value == null) {
			return "";
		}
		List<RangerDataMaskPolicyItem> rangerPolicyItems = (List<RangerDataMaskPolicyItem>) value;
		if(rangerPolicyItems==null || rangerPolicyItems.size()==0){
			return "";
		}
		String ret = jsonUtil.readListToString(rangerPolicyItems);
		if(ret == null) {
			return "";
		}
		return ret;
	}

	@SuppressWarnings("unchecked")
	private String processRowFilterPolicyItemForTrxLog(Object value) {
		if(value == null) {
			return "";
		}
		List<RangerRowFilterPolicyItem> rangerPolicyItems = (List<RangerRowFilterPolicyItem>) value;
		if(rangerPolicyItems==null || rangerPolicyItems.size()==0){
			return "";
		}
		String ret = jsonUtil.readListToString(rangerPolicyItems);
		if(ret == null) {
			return "";
		}
		return ret;
	}
	private String processIsEnabledClassFieldNameForTrxLog(Object value){
		if(value == null)
			return null;
		String isEnabled= String.valueOf(value);
			return isEnabled;
	}

	private boolean compareTwoDataMaskingPolicyItemList(String value, String oldValue) {
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
			List<RangerDataMaskPolicyItem> obj = mapper.readValue(value,
					new TypeReference<List<RangerDataMaskPolicyItem>>() {
					});
			List<RangerDataMaskPolicyItem> oldObj = mapper.readValue(oldValue,
					new TypeReference<List<RangerDataMaskPolicyItem>>() {
					});
			int oldListSize = oldObj.size();
			int listSize = obj.size();
			if(oldListSize != listSize) {
				return false;
			}
			for(RangerDataMaskPolicyItem polItem : obj) {
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

	private boolean compareTwoRowFilterPolicyItemList(String value, String oldValue) {
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
			List<RangerRowFilterPolicyItem> obj = mapper.readValue(value,
					new TypeReference<List<RangerRowFilterPolicyItem>>() {
					});
			List<RangerRowFilterPolicyItem> oldObj = mapper.readValue(oldValue,
					new TypeReference<List<RangerRowFilterPolicyItem>>() {
					});
			int oldListSize = oldObj.size();
			int listSize = obj.size();
			if(oldListSize != listSize) {
				return false;
			}
			for(RangerRowFilterPolicyItem polItem : obj) {
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
}
