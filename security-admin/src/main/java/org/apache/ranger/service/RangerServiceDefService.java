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

import java.util.ArrayList;
import java.util.List;

import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.XXServiceDefBase;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class RangerServiceDefService extends RangerServiceDefServiceBase<XXServiceDef, RangerServiceDef> {

	public RangerServiceDefService() {
		super();
	}

	@Override
	protected void validateForCreate(RangerServiceDef vObj) {

	}

	@Override
	protected void validateForUpdate(RangerServiceDef vObj, XXServiceDef entityObj) {

	}

	@Override
	protected XXServiceDef mapViewToEntityBean(RangerServiceDef vObj, XXServiceDef xObj, int OPERATION_CONTEXT) {
		return (XXServiceDef) super.mapViewToEntityBean(vObj, (XXServiceDefBase) xObj, OPERATION_CONTEXT);
	}

	@Override
	protected RangerServiceDef mapEntityToViewBean(RangerServiceDef vObj, XXServiceDef xObj) {
		return super.mapEntityToViewBean(vObj, (XXServiceDefBase) xObj);
	}

	public List<RangerServiceDef> getAllServiceDefs() {
		List<XXServiceDef> xxServiceDefList = daoMgr.getXXServiceDef().getAll();
		List<RangerServiceDef> serviceDefList = new ArrayList<RangerServiceDef>();

		for (XXServiceDef xxServiceDef : xxServiceDefList) {
			RangerServiceDef serviceDef = populateViewBean(xxServiceDef);
			serviceDefList.add(serviceDef);
		}
		return serviceDefList;
	}

	public RangerServiceDef getPopulatedViewObject(XXServiceDef xServiceDef) {
		return this.populateViewBean(xServiceDef);
	}
}
