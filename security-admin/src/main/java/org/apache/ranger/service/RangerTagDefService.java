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

package org.apache.ranger.service;

import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SearchField.DATA_TYPE;
import org.apache.ranger.common.SearchField.SEARCH_TYPE;
import org.apache.ranger.entity.XXTagDef;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.util.SearchFilter;
import org.springframework.stereotype.Service;

@Service
public class RangerTagDefService extends RangerTagDefServiceBase<XXTagDef, RangerTagDef> {

	public RangerTagDefService() {
		searchFields.add(new SearchField(SearchFilter.TAG_DEF_ID, "obj.id", DATA_TYPE.INTEGER, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.TAG_DEF_NAME, "obj.name", DATA_TYPE.STRING, SEARCH_TYPE.FULL));
	}
	
	@Override
	protected void validateForCreate(RangerTagDef vObj) {

	}

	@Override
	protected void validateForUpdate(RangerTagDef vObj, XXTagDef entityObj) {

	}

	public RangerTagDef getPopulatedViewObjject(XXTagDef xObj) {
		return populateViewBean(xObj);
	}

}
