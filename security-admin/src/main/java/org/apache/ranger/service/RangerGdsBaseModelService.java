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

import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.view.VTrxLogAttr;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.plugin.model.RangerGds.GdsShareStatus;
import org.apache.ranger.plugin.model.RangerGds.RangerGdsBaseModelObject;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.view.VXMessage;

import java.util.ArrayList;
import java.util.List;

public abstract class RangerGdsBaseModelService<T extends XXDBBase, V extends RangerGdsBaseModelObject> extends RangerAuditedModelService<T, V> {
    protected RangerGdsBaseModelService(int classType) {
        super(classType, -1);
    }

    protected RangerGdsBaseModelService(int classType, int parentClassType) {
        super(classType, parentClassType);

        searchFields.add(new SearchField(SearchFilter.GUID, "obj.guid", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.IS_ENABLED, "obj.isEnabled", SearchField.DATA_TYPE.BOOLEAN, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.CREATE_TIME_START, "obj.createTime", SearchField.DATA_TYPE.DATE, SearchField.SEARCH_TYPE.GREATER_EQUAL_THAN));
        searchFields.add(new SearchField(SearchFilter.CREATE_TIME_END, "obj.createTime", SearchField.DATA_TYPE.DATE, SearchField.SEARCH_TYPE.LESS_EQUAL_THAN));
        searchFields.add(new SearchField(SearchFilter.UPDATE_TIME_START, "obj.updateTime", SearchField.DATA_TYPE.DATE, SearchField.SEARCH_TYPE.GREATER_EQUAL_THAN));
        searchFields.add(new SearchField(SearchFilter.UPDATE_TIME_END, "obj.createTime", SearchField.DATA_TYPE.DATE, SearchField.SEARCH_TYPE.LESS_EQUAL_THAN));

        trxLogAttrs.put("description", new VTrxLogAttr("description", "Description"));
        trxLogAttrs.put("options", new VTrxLogAttr("options", "Options"));
        trxLogAttrs.put("additionalInfo", new VTrxLogAttr("additionalInfo", "Additional info"));
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
}
