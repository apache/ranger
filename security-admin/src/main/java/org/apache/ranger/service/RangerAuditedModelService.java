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

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.view.VTrxLogAttr;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.util.RangerEnumUtil;
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

	@Autowired
	RangerBizUtil bizUtil;

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

	public void createTransactionLog(XXTrxLog trxLog) {
		bizUtil.createTrxLog(Collections.singletonList(trxLog));
	}

	public void createTransactionLog(V obj, V oldObj, int action) {
		List<XXTrxLog> trxLogs = getTransactionLogs(obj, oldObj, action);

		if (trxLogs != null) {
			bizUtil.createTrxLog(trxLogs);
		}
	}

	private List<XXTrxLog> getTransactionLogs(V obj, V oldObj, int action) {
		if (obj == null || (action == OPERATION_UPDATE_CONTEXT && oldObj == null)) {
			return null;
		}

		List<XXTrxLog> ret = new ArrayList<>();

		try {
			String objName         = getObjectName(obj);
			int    parentClassType = getParentObjectType(obj, oldObj);
			String parentObjName   = getParentObjectName(obj, oldObj);
			Long   parentObjId     = getParentObjectId(obj, oldObj);

			for (VTrxLogAttr trxLog : trxLogAttrs.values()) {
				XXTrxLog xTrxLog = processFieldToCreateTrxLog(trxLog, objName, parentClassType, parentObjId, parentObjName, obj, oldObj, action);

				if (xTrxLog != null) {
					ret.add(xTrxLog);
				}
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

	private XXTrxLog processFieldToCreateTrxLog(VTrxLogAttr trxLogAttr, String objName, int parentClassType, Long parentObjId, String parentObjName, V obj, V oldObj, int action) {
		String actionString = "";
		String prevValue    = null;
		String newValue     = null;

		String value = getTrxLogAttrValue(obj, trxLogAttr);

		if (action == OPERATION_CREATE_CONTEXT) {
			actionString = "create";
			newValue     = value;
		} else if (action == OPERATION_DELETE_CONTEXT) {
			actionString = "delete";
			prevValue    = value;
		} else if (action == OPERATION_UPDATE_CONTEXT) {
			actionString = "update";
			prevValue    = getTrxLogAttrValue(oldObj, trxLogAttr);
			newValue     = value;
		}

		final XXTrxLog ret;

		if ((action == OPERATION_CREATE_CONTEXT || action == OPERATION_DELETE_CONTEXT) && StringUtils.isBlank(value)) {
			ret = null;
		} else if (skipTrxLogForAttribute(obj, oldObj, trxLogAttr)) {
			ret = null;
		} else if (StringUtils.equals(prevValue, newValue)) {
			ret = null;
		} else {
			ret = new XXTrxLog(classType, obj.getId(), objName, actionString, trxLogAttr.getAttribUserFriendlyName(), prevValue, newValue);

			ret.setParentObjectClassType(parentClassType);
			ret.setParentObjectId(parentObjId);
			ret.setParentObjectName(parentObjName);
		}

		return ret;
	}
}
