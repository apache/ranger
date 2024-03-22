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

/**
 * List wrapper class for VXAccessAudit
 *
 */

import org.apache.ranger.common.view.VList;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.List;

@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
public class VXUgsyncAuditInfoList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXUgsyncAuditInfo> vxUgsyncAuditInfoList = new ArrayList<VXUgsyncAuditInfo>();

    public VXUgsyncAuditInfoList() {
	super();
    }

    public VXUgsyncAuditInfoList(List<VXUgsyncAuditInfo> objList) {
	super(objList);
	this.vxUgsyncAuditInfoList = objList;
    }

    /**
     * @return the vXAccessAudits
     */
    public List<VXUgsyncAuditInfo> getVxUgsyncAuditInfoList() {
	return vxUgsyncAuditInfoList;
    }

    /**
     * @param vxUgsyncAuditInfoList
     *            the vXAccessAudits to set
     */
    public void setVxUgsyncAuditInfoList(List<VXUgsyncAuditInfo> vxUgsyncAuditInfoList) {
	this.vxUgsyncAuditInfoList = vxUgsyncAuditInfoList;
    }

    @Override
    public int getListSize() {
	if (vxUgsyncAuditInfoList != null) {
	    return vxUgsyncAuditInfoList.size();
	}
	return 0;
    }

    @Override
    public List<VXUgsyncAuditInfo> getList() {
	return vxUgsyncAuditInfoList;
    }

}
