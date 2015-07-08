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

import org.apache.commons.collections.Predicate;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerTaggedResource;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.util.SearchFilter;

import java.util.List;

public class TagPredicateUtil extends AbstractPredicateUtil {

	public TagPredicateUtil() { super(); }

	@Override
	public void addPredicates(SearchFilter filter, List<Predicate> predicates) {
		super.addPredicates(filter, predicates);

		addPredicateForTagDefId(filter.getParam(SearchFilter.TAG_DEF_ID), predicates);
		addPredicateForTagDefName(filter.getParam(SearchFilter.TAG_DEF_NAME), predicates);

		addPredicateForTagResourceServiceName(filter.getParam(SearchFilter.TAG_RESOURCE_SERVICE_NAME), predicates);
		addPredicateForTagResourceTimestamp(filter.getParamAsLong(SearchFilter.TAG_RESOURCE_TIMESTAMP), predicates);

		addPredicateForTagResourceId(filter.getParam(SearchFilter.TAG_RESOURCE_ID), predicates);
	}

	private Predicate addPredicateForTagDefId(final String id, List<Predicate> predicates) {
		if (StringUtils.isEmpty(id)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {

				boolean ret = false;

				if (object == null) {
					return ret;
				}

				if (object instanceof RangerTagDef) {
					RangerTagDef tagDef = (RangerTagDef) object;

					ret = StringUtils.equals(id, tagDef.getId().toString());
				}

				return ret;
			}
		};

		if (predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForTagDefName(final String name, List<Predicate> predicates) {
		if (name == null || StringUtils.isEmpty(name)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {

				boolean ret = false;

				if (object == null) {
					return ret;
				}

				if (object instanceof RangerTagDef) {
					RangerTagDef tagDef = (RangerTagDef) object;

					ret = StringUtils.equals(name, tagDef.getName());
				}

				return ret;
			}
		};

		if (predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForTagResourceServiceName(final String name, List<Predicate> predicates) {
		if (name == null || StringUtils.isEmpty(name)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {

				boolean ret = false;

				if (object == null) {
					return ret;
				}

				if (object instanceof RangerTaggedResource) {
					RangerTaggedResource rangerResource = (RangerTaggedResource) object;

					ret = StringUtils.equals(name, rangerResource.getKey().getServiceName());
				}

				return ret;
			}
		};

		if (predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}
	private Predicate addPredicateForTagResourceId(final String id, List<Predicate> predicates) {
		if (StringUtils.isEmpty(id)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {

				boolean ret = false;

				if (object == null) {
					return ret;
				}

				if (object instanceof RangerTaggedResource) {
					RangerTaggedResource rangerResource = (RangerTaggedResource) object;

					ret = StringUtils.equals(id, rangerResource.getId().toString());
				}

				return ret;
			}
		};

		if (predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}
	private Predicate addPredicateForTagResourceTimestamp(final Long lastTimestamp, List<Predicate> predicates) {
		final int uploadInterval = 1*1000;
		// Assumption: it may take maximum of one second for a taggedResource to be persisted after the timestamp
		// was generated for it. The round-trip time is already taken into consideration by client.


		if (lastTimestamp == null) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {

				boolean ret = false;

				if (object == null) {
					return ret;
				}

				if (object instanceof RangerTaggedResource) {
					RangerTaggedResource rangerResource = (RangerTaggedResource) object;

					ret = rangerResource.getUpdateTime().getTime() >= (lastTimestamp - uploadInterval);
				}

				return ret;
			}
		};

		if (predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}}
