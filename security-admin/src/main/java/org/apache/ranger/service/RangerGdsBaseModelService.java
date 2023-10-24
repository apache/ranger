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
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.view.VTrxLogAttr;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.plugin.model.RangerGds.GdsShareStatus;
import org.apache.ranger.plugin.model.RangerGds.RangerGdsBaseModelObject;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.view.VXMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class RangerGdsBaseModelService<T extends XXDBBase, V extends RangerGdsBaseModelObject> extends RangerBaseModelService<T, V> {
    private static final Logger LOG = LoggerFactory.getLogger(RangerGdsBaseModelService.class);

    @Autowired
    RangerDataHistService dataHistService;

    @Autowired
    RangerBizUtil bizUtil;

    protected final Map<String, VTrxLogAttr> trxLogAttrs = new HashMap<>();
    private   final int                      classType;

    protected RangerGdsBaseModelService(int classType) {
        this.classType = classType;

        searchFields.add(new SearchField(SearchFilter.GUID,              "obj.guid",       SearchField.DATA_TYPE.STRING,  SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.IS_ENABLED,        "obj.isEnabled",  SearchField.DATA_TYPE.BOOLEAN, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.CREATE_TIME_START, "obj.createTime", SearchField.DATA_TYPE.DATE,    SearchField.SEARCH_TYPE.GREATER_EQUAL_THAN));
        searchFields.add(new SearchField(SearchFilter.CREATE_TIME_END,   "obj.createTime", SearchField.DATA_TYPE.DATE,    SearchField.SEARCH_TYPE.LESS_EQUAL_THAN));
        searchFields.add(new SearchField(SearchFilter.UPDATE_TIME_START, "obj.updateTime", SearchField.DATA_TYPE.DATE,    SearchField.SEARCH_TYPE.GREATER_EQUAL_THAN));
        searchFields.add(new SearchField(SearchFilter.UPDATE_TIME_END,   "obj.createTime", SearchField.DATA_TYPE.DATE,    SearchField.SEARCH_TYPE.LESS_EQUAL_THAN));

        trxLogAttrs.put("description",    new VTrxLogAttr("description", "Description", false));
        trxLogAttrs.put("options",        new VTrxLogAttr("options", "Options", false));
        trxLogAttrs.put("additionalInfo", new VTrxLogAttr("additionalInfo", "Additional info", false));
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

    public static List<VXMessage> getOrCreateMessageList(List<VXMessage> msgList) {
        if (msgList == null) {
            msgList = new ArrayList<>();
        }

        return msgList;
    }

    public static GdsShareStatus toShareStatus(short ordinal) {
        GdsShareStatus ret = GdsShareStatus.NONE;

        for (GdsShareStatus status : GdsShareStatus.values()) {
            if (status.ordinal() == ordinal) {
                ret = status;

                break;
            }
        }

        return ret;
    }

    private void createTransactionLog(V obj, V oldObj, int action) {
        List<XXTrxLog> trxLogs = new ArrayList<>();
        String         objName = getObjectName(obj);

        for (Field field : obj.getClass().getDeclaredFields()) {
            if (!trxLogAttrs.containsKey(field.getName())) {
                continue;
            }

            XXTrxLog xTrxLog = processFieldToCreateTrxLog(field, objName, obj, oldObj, action);

            if (xTrxLog != null) {
                trxLogs.add(xTrxLog);
            }
        }

        for (Field field : obj.getClass().getSuperclass().getDeclaredFields()) {
            if (!trxLogAttrs.containsKey(field.getName())) {
                continue;
            }

            XXTrxLog xTrx = processFieldToCreateTrxLog(field, objName, obj, oldObj, action);

            if (xTrx != null) {
                trxLogs.add(xTrx);
            }
        }

        bizUtil.createTrxLog(trxLogs);
    }

    private String getObjectName(V obj) {
        try {
            Field nameField = obj.getClass().getDeclaredField("name");

            nameField.setAccessible(true);

            return Objects.toString(nameField.get(obj));
        } catch (NoSuchFieldException | IllegalAccessException excp) {
            // ignore
            return null;
        }
    }

    private XXTrxLog processFieldToCreateTrxLog(Field field, String objName, V obj, V oldObj, int action) {
        field.setAccessible(true);

        String actionString = "";
        String attrName     = null;
        String prevValue    = null;
        String newValue     = null;
        String fieldName    = field.getName();

        try {
            VTrxLogAttr vTrxLogAttr = trxLogAttrs.get(fieldName);
            String      value       = toString(field.get(obj));

            attrName = vTrxLogAttr.getAttribUserFriendlyName();

            if (action == OPERATION_CREATE_CONTEXT) {
                actionString = "create";

                if (StringUtils.isNotBlank(value)) {
                    newValue = value;
                }
            } else if (action == OPERATION_DELETE_CONTEXT) {
                actionString = "delete";
                prevValue    = value;
            } else if (action == OPERATION_UPDATE_CONTEXT) {
                actionString = "update";
                prevValue    = toString(field.get(oldObj));
                newValue     = value;
            }
        } catch (IllegalArgumentException | IllegalAccessException e) {
            LOG.error("Process field to create trx log failure.", e);
        }

        XXTrxLog ret = null;

        if (!StringUtils.equals(prevValue, newValue)) {
            ret = new XXTrxLog();

            ret.setAction(actionString);
            ret.setAttributeName(attrName);
            ret.setPreviousValue(prevValue);
            ret.setNewValue(newValue);
            ret.setObjectClassType(classType);
            ret.setObjectId(obj.getId());
            ret.setObjectName(objName);
        }

        return ret;
    }

    private String toString(Object obj) {
        if (obj instanceof String) {
            return (String) obj;
        } else if (obj instanceof Serializable) {
            try {
                return JsonUtilsV2.objToJson((Serializable) obj);
            } catch (Exception excp) {
                // ignore
            }
        }

        return Objects.toString(obj);
    }
}
