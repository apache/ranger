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

import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.plugin.model.RangerGds.GdsShareStatus;
import org.apache.ranger.plugin.model.RangerGds.RangerGdsBaseModelObject;
import org.apache.ranger.view.VXMessage;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

public abstract class RangerGdsBaseModelService<T extends XXDBBase, V extends RangerGdsBaseModelObject> extends RangerBaseModelService<T, V> {
    @Autowired
    RangerDataHistService dataHistService;

    public void createObjectHistory(V current, V former, int action) {
        switch (action) {
            case RangerServiceService.OPERATION_CREATE_CONTEXT:
                dataHistService.createObjectDataHistory(current, RangerDataHistService.ACTION_CREATE);
            break;

            case RangerServiceService.OPERATION_UPDATE_CONTEXT:
                dataHistService.createObjectDataHistory(current, RangerDataHistService.ACTION_UPDATE);
            break;

            case RangerServiceService.OPERATION_DELETE_CONTEXT:
                dataHistService.createObjectDataHistory(current == null ? former : current, RangerDataHistService.ACTION_DELETE);
            break;
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
}
