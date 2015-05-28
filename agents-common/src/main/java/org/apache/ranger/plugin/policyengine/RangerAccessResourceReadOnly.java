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

package org.apache.ranger.plugin.policyengine;

import org.apache.ranger.plugin.model.RangerServiceDef;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class RangerAccessResourceReadOnly implements RangerAccessResource {

	private final RangerAccessResource source;
	private final Set<String> keys;
	private final Map<String, String> map;

	public RangerAccessResourceReadOnly(final RangerAccessResource source) {
		this.source = source;

		// Cached here for reducing access overhead
		this.keys = Collections.unmodifiableSet(source.getKeys());
		this.map = Collections.unmodifiableMap(source.getAsMap());
	}

	public String getOwnerUser() { return source.getOwnerUser(); }

	public boolean exists(String name) { return source.exists(name); }

	public String getValue(String name) { return source.getValue(name); }

	public Set<String> getKeys() { return keys; }

	public String getLeafName(RangerServiceDef serviceDef) { return source.getLeafName(serviceDef); }

	public String getAsString(RangerServiceDef serviceDef) { return source.getAsString(serviceDef); }

	public Map<String, String> getAsMap() { return map; }

	public RangerAccessResource getReadOnlyCopy() { return this; }
}
