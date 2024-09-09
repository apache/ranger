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

package org.apache.ranger.plugin.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown=true)
public class RangerServiceResourceWithTags extends RangerServiceResource implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	private List<RangerTag>	associatedTags;

	public List<RangerTag> getAssociatedTags() {
		return associatedTags;
	}

	public void setAssociatedTags(List<RangerTag> associatedTags) {
		this.associatedTags = associatedTags;
	}

	@Override
	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerServiceResourceWithTags={ ");

		super.toString(sb);

		sb.append("associatedTags=[");
		if (associatedTags != null) {
			String prefix = "";

			for (RangerTag associatedTag : associatedTags) {
	            sb.append(prefix);

				associatedTag.toString(sb);

				prefix = ", ";
	        }
		}
		sb.append("] ");

		sb.append(" }");

		return sb;
	}
}
