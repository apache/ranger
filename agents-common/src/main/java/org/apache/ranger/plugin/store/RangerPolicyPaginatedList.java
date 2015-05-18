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

package org.apache.ranger.plugin.store;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;

import java.util.ArrayList;
import java.util.List;

public class RangerPolicyPaginatedList extends PList {
	private static final long serialVersionUID = 1L;

	List<RangerPolicy> policies = new ArrayList<RangerPolicy>();

	public RangerPolicyPaginatedList() {
		super();
	}

	public RangerPolicyPaginatedList(List<RangerPolicy> objList) {
		super(objList);
		this.policies = objList;
	}

	public List<RangerPolicy> getPolicies() {
		return policies;
	}

	public void setPolicies(List<RangerPolicy> policies) {
		this.policies = policies;
	}

	@Override
	public int getListSize() {
		if (policies != null) {
			return policies.size();
		}
		return 0;
	}

	@Override
	public List<?> getList() {
		return policies;
	}
}
