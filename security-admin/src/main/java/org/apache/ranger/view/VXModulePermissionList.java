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

import java.util.ArrayList;
import java.util.List;

import org.apache.ranger.common.view.VList;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class VXModulePermissionList extends VList {

	private static final long serialVersionUID = 1L;

	List<VXModulePermission> vXModulePermissionList = new ArrayList<VXModulePermission>();

	public VXModulePermissionList() {
		super();
	}

	public VXModulePermissionList(List<VXModulePermission> objList) {
		super(objList);
		this.vXModulePermissionList = objList;
	}

	@JsonProperty("vXModulePermissionList")
	public List<VXModulePermission> getvXModulePermissionList() {
		return vXModulePermissionList;
	}

	@JsonProperty("vXModulePermissionList")
	public void setvXModulePermissionList(List<VXModulePermission> vXModulePermissionList) {
		this.vXModulePermissionList = vXModulePermissionList;
	}

	@Override
	public int getListSize() {
		if(vXModulePermissionList != null) {
			return vXModulePermissionList.size();
		}
		return 0;
	}

	@Override
	public List<VXModulePermission> getList() {
		return vXModulePermissionList;
	}
}
