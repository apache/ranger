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
public class VXAuditRecordList extends VList {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	List<VXAuditRecord> vXAuditRecords = new ArrayList<VXAuditRecord>();

	@JsonProperty("vXAuditRecords")
	public List<VXAuditRecord> getvAudits() {
		return vXAuditRecords;
	}

	@JsonProperty("vXAuditRecords")
	public void setvAudits(List<VXAuditRecord> vXAuditRecords) {
		this.vXAuditRecords = vXAuditRecords;
	}

	public VXAuditRecordList() {
		super();
	}

	@Override
	public int getListSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<?> getList() {
		// TODO Auto-generated method stub
		return null;
	}
}
