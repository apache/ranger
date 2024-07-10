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

import java.util.ArrayList;
import java.util.List;


import org.apache.ranger.common.view.VList;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class VXTrxLogList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXTrxLog> vXTrxLogs = new ArrayList<VXTrxLog>();

    public VXTrxLogList() {
	super();
    }

    public VXTrxLogList(List<VXTrxLog> objList) {
	super(objList);
	this.vXTrxLogs = objList;
    }

    /**
     * @return the vXTrxLogs
     */
    @JsonProperty("vXTrxLogs")
    public List<VXTrxLog> getVXTrxLogs() {
	return vXTrxLogs;
    }

    /**
     * @param vXTrxLogs
     *            the vXTrxLogs to set
     */
    @JsonProperty("vXTrxLogs")
    public void setVXTrxLogs(List<VXTrxLog> vXTrxLogs) {
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
    public List<VXTrxLog> getList() {
	return vXTrxLogs;
    }

}
