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

package org.apache.ranger.view;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.ranger.common.view.VList;
import org.apache.ranger.plugin.model.RangerAuditMetrics;

import java.util.ArrayList;
import java.util.List;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class RangerAuditMetricsList extends VList {
    private static final long serialVersionUID = 1L;

    List<RangerAuditMetrics> rangerAuditMetricsList = new ArrayList<>();

    public RangerAuditMetricsList() {
        super();
    }

    public RangerAuditMetricsList(List<RangerAuditMetrics> objList) {
        super(objList);
        this.rangerAuditMetricsList = objList;
    }

    public List<RangerAuditMetrics> getRangerAuditMetricsList() {
        return rangerAuditMetricsList;
    }

    public void setRangerAuditMetricsList(List<RangerAuditMetrics> auditMetrics) {
        this.rangerAuditMetricsList = auditMetrics;
    }

    @Override
    public int getListSize() {
        if (rangerAuditMetricsList != null) {
            return rangerAuditMetricsList.size();
        }
        return 0;
    }

    @Override
    public List<?> getList() {
        return rangerAuditMetricsList;
    }
}
