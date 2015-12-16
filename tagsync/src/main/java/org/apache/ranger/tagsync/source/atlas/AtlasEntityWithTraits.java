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

package org.apache.ranger.tagsync.source.atlas;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class AtlasEntityWithTraits {

	private final IReferenceableInstance entity;
	private final List<IStruct> traits;

	public AtlasEntityWithTraits(IReferenceableInstance entity, List<IStruct> traits) {
		this.entity = entity;
		this.traits = traits;
	}

	public IReferenceableInstance getEntity() {
		return entity;
	}

	public List<IStruct> getAllTraits() {
		return traits == null ? new LinkedList<IStruct>() : traits;
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public void toString(StringBuilder sb) {

		sb.append("AtlasEntityWithTraits={ ");

		sb.append("Entity-Id: " + entity.getId()._getId()).append(", ")
				.append("Entity-Type: " + entity.getTypeName()).append(", ");

		sb.append("Entity-Values={ ");
		try {
			for (Map.Entry<String, Object> entry : entity.getValuesMap().entrySet()) {
				sb.append("{").append(entry.getKey()).append(", ").append(entry.getValue()).append("}, ");
			}
		} catch (AtlasException exception) {
				// Ignore
		}
		sb.append(" }");

		sb.append(", Entity-Traits={ ");
		for (IStruct trait : traits) {
			try {
				sb.append("{traitType=").append(trait.getTypeName()).append(", ");
				Map<String, Object> traitValues = trait.getValuesMap();
				sb.append("{");
				for (Map.Entry<String, Object> valueEntry : traitValues.entrySet()) {
					sb.append("{").append(valueEntry.getKey()).append(", ").append(valueEntry.getValue()).append("}");
				}
				sb.append("}");

				sb.append(" }");
			} catch (AtlasException exception) {
				// Ignore
			}
		}
		sb.append(" }");

		sb.append(" }");

	}

}
