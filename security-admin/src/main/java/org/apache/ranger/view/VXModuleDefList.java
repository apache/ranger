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
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class VXModuleDefList extends VList {

	private static final long serialVersionUID = 1L;

	List<VXModuleDef> vXModuleDef = new ArrayList<VXModuleDef>();

	public VXModuleDefList() {
		super();
	}

	public VXModuleDefList(List<VXModuleDef> objList) {
		super(objList);
		this.vXModuleDef = objList;
	}

	/**
	 * @return the vXModuleDef
	 */
	public List<VXModuleDef> getvXModuleDef() {
		return vXModuleDef;
	}

	/**
	 * @param vXModuleDef the vXModuleDef to set
	 */
	public void setvXModuleDef(List<VXModuleDef> vXModuleDef) {
		this.vXModuleDef = vXModuleDef;
	}

	@Override
	public int getListSize() {
		if (vXModuleDef != null) {
			return vXModuleDef.size();
		}
		return 0;
	}

	@Override
	public List<VXModuleDef> getList() {
		return vXModuleDef;
	}

}
