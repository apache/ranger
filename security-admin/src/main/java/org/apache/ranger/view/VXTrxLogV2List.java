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

package org.apache.ranger.view;

import org.apache.ranger.common.view.VList;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class VXTrxLogV2List extends VList {
	private static final long serialVersionUID = 1L;
    List<VXTrxLogV2> vXTrxLogs = new ArrayList<>();

    public VXTrxLogV2List() {
        super();
    }

    public VXTrxLogV2List(List<VXTrxLogV2> objList) {
        super(objList);
        this.vXTrxLogs = objList;
    }

    /**
     * @return the vXTrxLogs
     */
    @JsonProperty("vXTrxLogs")
    public List<VXTrxLogV2> getVXTrxLogs() {
        return vXTrxLogs;
    }

    /**
     * @param vXTrxLogs
     *            the vXTrxLogs to set
     */
    @JsonProperty("vXTrxLogs")
    public void setVXTrxLogs(List<VXTrxLogV2> vXTrxLogs) {
        this.vXTrxLogs = vXTrxLogs;
    }

    @Override
    public int getListSize() {
	if (vXTrxLogs != null) {
	    return vXTrxLogs.size();
	}
        return 0;
    }

    @Override
    public List<VXTrxLogV2> getList() {
        return vXTrxLogs;
    }

}
