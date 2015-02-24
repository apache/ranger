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

package org.apache.ranger.rest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;

public class ValidationTestUtils {
	
	Map<String, String> createMap(String[] keys) {
		Map<String, String> result = new HashMap<String, String>();
		for (String key : keys) {
			result.put(key, "valueof-" + key);
		}
		return result;
	}

	// helper methods for tests
	List<RangerServiceConfigDef> createServiceConditionDefs(Object[][] input) {
		List<RangerServiceConfigDef> result = new ArrayList<RangerServiceDef.RangerServiceConfigDef>();
		
		for (Object data[] : input) {
			RangerServiceConfigDef aConfigDef = mock(RangerServiceConfigDef.class);
			when(aConfigDef.getName()).thenReturn((String)data[0]);
			when(aConfigDef.getMandatory()).thenReturn((boolean)data[1]);
			result.add(aConfigDef);
		}
		
		return result;
	}
	
}
