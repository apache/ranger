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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.view.VTrxLogAttr;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXDataMaskTypeDef;
import org.apache.ranger.entity.XXTrxLogV2;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.util.RangerEnumUtil;
import org.apache.ranger.view.VXTrxLogV2.ObjectChangeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.*;

public abstract class RangerAuditedModelService<T extends XXDBBase, V extends RangerBaseModelObject> extends RangerBaseModelService<T, V> {
	private static final Logger LOG = LoggerFactory.getLogger(RangerAuditedModelService.class);

	@Autowired
	RangerDataHistService dataHistService;

	@Autowired
	RangerEnumUtil xaEnumUtil;
	private final int               classType;
	private final int               parentClassType;
	private final List<VTrxLogAttr> objNameAttrs = new ArrayList<>();

	protected final Map<String, VTrxLogAttr> trxLogAttrs = new HashMap<>();
	protected final String                   hiddenPasswordString;

	protected RangerAuditedModelService(int classType) {
		this(classType, 0);
	}

	protected RangerAuditedModelService(int classType, int parentClassType) {
		super();

		this.classType            = classType;
		this.parentClassType      = parentClassType;
		this.hiddenPasswordString = PropertiesUtil.getProperty("ranger.password.hidden", "*****");

		LOG.debug("RangerAuditedModelService({}, {})", this.classType, this.parentClassType);
	}

	@PostConstruct
	public void init() {
		for (VTrxLogAttr vTrxLog : trxLogAttrs.values()) {
			if (vTrxLog.isObjName()) {
				objNameAttrs.add(vTrxLog);
			}
		}

		if (objNameAttrs.isEmpty()) {
			objNameAttrs.add(new VTrxLogAttr("name", "Name", false, true));
		}
	}

	public void onObjectChange(V current, V former, int action) {
		switch (action) {
			case RangerServiceService.OPERATION_CREATE_CONTEXT:
				dataHistService.createObjectDataHistory(current, RangerDataHistService.ACTION_CREATE);
				break;

			case RangerServiceService.OPERATION_UPDATE_CONTEXT:
				dataHistService.createObjectDataHistory(current, RangerDataHistService.ACTION_UPDATE);
				break;

			case RangerServiceService.OPERATION_DELETE_CONTEXT:
				if (current == null) {
					current = former;
				}

				dataHistService.createObjectDataHistory(current, RangerDataHistService.ACTION_DELETE);
				break;
		}

		if (current != null && (former != null || action != OPERATION_UPDATE_CONTEXT) && action != 0) {
			createTransactionLog(current, former, action);
		}
	}

	public void createTransactionLog(XXTrxLogV2 trxLog, String attrName, String oldValue, String newValue) {
		try {
			ObjectChangeInfo objChangeInfo = new ObjectChangeInfo();

			objChangeInfo.addAttribute(attrName, oldValue, newValue);

			trxLog.setChangeInfo(JsonUtilsV2.objToJson(objChangeInfo));
		} catch (Exception excp) {
			LOG.warn("failed to convert attribute change info to json");
		}

		bizUtil.createTrxLog(Collections.singletonList(trxLog));
	}

	public void createTransactionLog(XXTrxLogV2 trxLog) {
		bizUtil.createTrxLog(Collections.singletonList(trxLog));
	}

	public void createTransactionLog(V obj, V oldObj, int action) {
		List<XXTrxLogV2> trxLogs = getTransactionLogs(obj, oldObj, action);

		if (trxLogs != null) {
			bizUtil.createTrxLog(trxLogs);
		}
	}

	private List<XXTrxLogV2> getTransactionLogs(V obj, V oldObj, int action) {
		if (obj == null || (action == OPERATION_UPDATE_CONTEXT && oldObj == null)) {
			return null;
		}

		List<XXTrxLogV2> ret = new ArrayList<>();

		try {
			ObjectChangeInfo objChangeInfo = new ObjectChangeInfo();

			for (VTrxLogAttr trxLog : trxLogAttrs.values()) {
				processFieldToCreateTrxLog(trxLog, obj, oldObj, action, objChangeInfo);
			}

			if(objChangeInfo.getAttributes() != null && objChangeInfo.getAttributes().size() > 0) {
				ret.add(new XXTrxLogV2(classType, obj.getId(), getObjectName(obj), getParentObjectType(obj, oldObj), getParentObjectId(obj, oldObj), getParentObjectName(obj, oldObj), toActionString(action), JsonUtilsV2.objToJson(objChangeInfo)));
			}
		} catch (Exception excp) {
			LOG.warn("failed to get transaction log for object: type=" + obj.getClass().getName() + ", id=" + obj.getId(), excp);
		}

		return ret;
	}

	public int getParentObjectType(V obj, V oldObj) {
		return parentClassType;
	}

	public String getParentObjectName(V obj, V oldObj) {
		return null;
	}

	public Long getParentObjectId(V obj, V oldObj) {
		return null;
	}

	public boolean skipTrxLogForAttribute(V obj, V oldObj, VTrxLogAttr trxLogAttr) {
		return false;
	}

	public String getTrxLogAttrValue(V obj, VTrxLogAttr trxLogAttr) {
		return trxLogAttr.getAttrValue(obj, xaEnumUtil);
	}

	private String getObjectName(V obj) {
		String ret = null;

		for (VTrxLogAttr attr : objNameAttrs) {
			ret = attr.getAttrValue(obj, xaEnumUtil);

			if (StringUtils.isNotBlank(ret)) {
				break;
			}
		}

		return ret;
	}

	private void processFieldToCreateTrxLog(VTrxLogAttr trxLogAttr, V obj, V oldObj, int action, ObjectChangeInfo objChangeInfo) {
		if (skipTrxLogForAttribute(obj, oldObj, trxLogAttr)) {
			return;
		}

		String value = getTrxLogAttrValue(obj, trxLogAttr);

		if ("dataMaskPolicyItems".equals(trxLogAttr.getAttribName())) {
			value = addLabelToDataMaskTxnPolicy(obj, value);
		}

		if ((action == OPERATION_CREATE_CONTEXT || action == OPERATION_DELETE_CONTEXT) && StringUtils.isBlank(value)) {
			return;
		}

		final String prevValue;
		final String newValue;

		if (action == OPERATION_CREATE_CONTEXT) {
			prevValue = null;
			newValue  = value;
		} else if (action == OPERATION_DELETE_CONTEXT) {
			prevValue = value;
			newValue  = null;
		} else if (action == OPERATION_UPDATE_CONTEXT) {
			prevValue = getTrxLogAttrValue(oldObj, trxLogAttr);
			newValue  = value;
		} else if (action == OPERATION_IMPORT_CREATE_CONTEXT) {
			prevValue = null;
			newValue  = value;
		} else if (action == OPERATION_IMPORT_DELETE_CONTEXT) {
			prevValue = value;
			newValue  = null;
		} else {
			prevValue = null;
			newValue  = null;
		}

		if (StringUtils.equals(prevValue, newValue) || (StringUtils.isEmpty(prevValue) && StringUtils.isEmpty(newValue))) {
			return;
		}

		objChangeInfo.addAttribute(trxLogAttr.getAttribUserFriendlyName(), prevValue, newValue);
	}

	public String addLabelToDataMaskTxnPolicy(V obj, String value) {
		if (obj instanceof RangerPolicy && ((RangerPolicy) obj).getDataMaskPolicyItems() != null) {
			for (RangerDataMaskPolicyItem policyItem : ((RangerPolicy) obj).getDataMaskPolicyItems()) {
				if (policyItem.getDataMaskInfo() != null && policyItem.getDataMaskInfo().getDataMaskType() != null) {
					String dataMaskType = policyItem.getDataMaskInfo().getDataMaskType();
					List<XXDataMaskTypeDef> dataMaskDefs = daoMgr.getXXDataMaskTypeDef().getAll();
					if (CollectionUtils.isNotEmpty(dataMaskDefs)) {
						for (XXDataMaskTypeDef dataMaskTypeDef : dataMaskDefs) {
							if (dataMaskTypeDef.getName().equalsIgnoreCase(dataMaskType)) {
								String label = dataMaskTypeDef.getLabel();
								StringBuilder sbValue = new StringBuilder(value);
								label = ",\"DataMasklabel\":\"" + label + "\"";
								int sbValueIndex = sbValue.lastIndexOf("}]");
								sbValue.insert(sbValueIndex, label);
								value = sbValue.toString();
								break;
							}
						}
					}
				}
			}
		}
		return value;
	}

	private String toActionString(int action) {
		switch (action) {
			case OPERATION_CREATE_CONTEXT:
				return "create";
			case OPERATION_UPDATE_CONTEXT:
				return "update";
			case OPERATION_DELETE_CONTEXT:
				return "delete";
			case OPERATION_IMPORT_CREATE_CONTEXT:
				return "Import Create";
			case OPERATION_IMPORT_DELETE_CONTEXT:
				return "Import Delete";
		}

		return "unknown";
	}
}
