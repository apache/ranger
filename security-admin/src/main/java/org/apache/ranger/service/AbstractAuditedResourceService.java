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

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.view.VTrxLogAttr;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXTrxLogV2;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.util.RangerEnumUtil;
import org.apache.ranger.view.VXDataObject;
import org.apache.ranger.view.VXTrxLogV2.AttributeChangeInfo;
import org.apache.ranger.view.VXTrxLogV2.ObjectChangeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.*;

import static org.apache.ranger.service.RangerBaseModelService.OPERATION_CREATE_CONTEXT;
import static org.apache.ranger.service.RangerBaseModelService.OPERATION_DELETE_CONTEXT;
import static org.apache.ranger.service.RangerBaseModelService.OPERATION_UPDATE_CONTEXT;

public abstract class AbstractAuditedResourceService<T extends XXDBBase, V extends VXDataObject> extends AbstractBaseResourceService<T, V> {
	protected static final Logger logger = LoggerFactory.getLogger(AbstractAuditedResourceService.class);

	@Autowired
	JSONUtil jsonUtil;

	@Autowired
	RangerEnumUtil xaEnumUtil;

	protected final Map<String, VTrxLogAttr> trxLogAttrs = new HashMap<>();
	protected final String                   hiddenPasswordString;
	private   final int                      classType;
	private   final int                      parentClassType;
	private   final List<VTrxLogAttr>        objNameAttrs = new ArrayList<>();

	protected AbstractAuditedResourceService(int classType) {
		this(classType, -1);
	}

	protected AbstractAuditedResourceService(int classType, int parentClassType) {
		this.classType            = classType;
		this.parentClassType      = parentClassType;
		this.hiddenPasswordString = PropertiesUtil.getProperty("ranger.password.hidden", "*****");
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

	public void createTransactionLog(XXTrxLogV2 trxLog) {
		bizUtil.createTrxLog(Collections.singletonList(trxLog));
	}

	public void createTransactionLog(XXTrxLogV2 trxLog, String attrName, String oldValue, String newValue) {
		try {
			ObjectChangeInfo objChangeInfo = new ObjectChangeInfo();

			if ("Password".equalsIgnoreCase(attrName)) {
				oldValue = hiddenPasswordString;
				newValue = hiddenPasswordString;
			}

			objChangeInfo.addAttribute(attrName, oldValue, newValue);

			trxLog.setChangeInfo(JsonUtilsV2.objToJson(objChangeInfo));
		} catch (Exception excp) {
			logger.warn("failed to convert attribute change info to json");
		}

		bizUtil.createTrxLog(Collections.singletonList(trxLog));
	}

	public void createTransactionLog(V obj, V oldObj, int action, Long userId) {
		List<XXTrxLogV2> trxLogList = getTransactionLog(obj, oldObj, action);

		if (trxLogList != null) {
			for (XXTrxLogV2 trxLog : trxLogList) {
				trxLog.setAddedByUserId(userId);
			}

			bizUtil.createTrxLog(trxLogList);
		}

		createTransactionLog(obj, null, action);
	}

	public void createTransactionLog(V obj, V oldObj, int action) {
		List<XXTrxLogV2> trxLogList = getTransactionLog(obj, oldObj, action);

		if (trxLogList != null) {
			bizUtil.createTrxLog(trxLogList);
		}
	}

	public List<XXTrxLogV2> getTransactionLog(V obj, V oldObj, int action) {
		if (obj == null || (action == OPERATION_UPDATE_CONTEXT && oldObj == null)) {
			return null;
		}

		List<XXTrxLogV2> trxLogList = new ArrayList<>();

		try {
			ObjectChangeInfo objChangeInfo = new ObjectChangeInfo();

			for (VTrxLogAttr trxLog : trxLogAttrs.values()) {
				processFieldToCreateTrxLog(trxLog, obj, oldObj, action, objChangeInfo);
			}

			if (objChangeInfo.getAttributes() != null) {
				for (AttributeChangeInfo changeInfo : objChangeInfo.getAttributes()) {
					if ("Password".equalsIgnoreCase(changeInfo.getAttributeName())) {
						changeInfo.setNewValue(hiddenPasswordString);
						changeInfo.setOldValue(hiddenPasswordString);
					}
				}

				trxLogList.add(new XXTrxLogV2(classType, obj.getId(), getObjectName(obj), getParentObjectType(obj, oldObj), getParentObjectId(obj, oldObj), getParentObjectName(obj, oldObj), toActionString(action), JsonUtilsV2.objToJson(objChangeInfo)));
			}
		} catch (Exception e) {
			logger.warn("failed to get transaction log for object: type=" + obj.getClass().getName() + ", id=" + obj.getId(), e);
		}

		return trxLogList;
	}

	public String getObjectName(V obj) {
		String ret = null;

		for (VTrxLogAttr attr : objNameAttrs) {
			ret = attr.getAttrValue(obj, xaEnumUtil);

			if (StringUtils.isNotBlank(ret)) {
				break;
			}
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

	private void processFieldToCreateTrxLog(VTrxLogAttr trxLogAttr, V obj, V oldObj, int action, ObjectChangeInfo objChangeInfo) {
		if (skipTrxLogForAttribute(obj, oldObj, trxLogAttr)) {
			return;
		}

		String value = getTrxLogAttrValue(obj, trxLogAttr);

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
		} else {
			prevValue = null;
			newValue  = null;
		}

		if ((StringUtils.isEmpty(prevValue) && StringUtils.isEmpty(newValue)) || StringUtils.equals(prevValue, newValue)) {
			return;
		}

		objChangeInfo.addAttribute(trxLogAttr.getAttribUserFriendlyName(), prevValue, newValue);
	}

	private String toActionString(int action) {
		switch (action) {
			case OPERATION_CREATE_CONTEXT:
				return "create";
			case OPERATION_UPDATE_CONTEXT:
				return "update";
			case OPERATION_DELETE_CONTEXT:
				return "delete";
		}

		return "unknown";
	}
}
