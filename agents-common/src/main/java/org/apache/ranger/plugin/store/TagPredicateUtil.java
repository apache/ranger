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
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagResourceMap;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.util.SearchFilter;

import java.util.List;

public class TagPredicateUtil extends AbstractPredicateUtil {

	public TagPredicateUtil() { super(); }

	@Override
	public void addPredicates(SearchFilter filter, List<Predicate> predicates) {
		super.addPredicates(filter, predicates);

		addPredicateForTagDefId(filter.getParam(SearchFilter.TAG_DEF_ID), predicates);
		addPredicateForTagDefExternalId(filter.getParam(SearchFilter.TAG_DEF_EXTERNAL_ID), predicates);
		addPredicateForTagDefName(filter.getParam(SearchFilter.TAG_DEF_NAME), predicates);

		addPredicateForTagId(filter.getParam(SearchFilter.TAG_ID), predicates);
		addPredicateForTagExternalId(filter.getParam(SearchFilter.TAG_EXTERNAL_ID), predicates);
		addPredicateForTagName(filter.getParam(SearchFilter.TAG_NAME), predicates);

		addPredicateForResourceId(filter.getParam(SearchFilter.TAG_RESOURCE_ID), predicates);
		addPredicateForResourceExternalId(filter.getParam(SearchFilter.TAG_RESOURCE_EXTERNAL_ID), predicates);
		addPredicateForServiceResourceServiceName(filter.getParam(SearchFilter.TAG_RESOURCE_SERVICE_NAME), predicates);
		addPredicateForResourceSignature(filter.getParam(SearchFilter.TAG_RESOURCE_SIGNATURE), predicates);

		addPredicateForTagResourceMapId(filter.getParam(SearchFilter.TAG_MAP_ID), predicates);
		addPredicateForTagResourceMapResourceId(filter.getParam(SearchFilter.TAG_MAP_RESOURCE_ID), predicates);
		addPredicateForTagResourceMapTagId(filter.getParam(SearchFilter.TAG_MAP_TAG_ID), predicates);
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

	private Predicate addPredicateForTagDefExternalId(final String externalId, List<Predicate> predicates) {
		if (externalId == null || StringUtils.isEmpty(externalId)) {
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

					ret = StringUtils.equals(externalId, tagDef.getGuid());
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

	private Predicate addPredicateForTagId(final String id, List<Predicate> predicates) {
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

				if (object instanceof RangerTag) {
					RangerTag tag = (RangerTag) object;

					ret = StringUtils.equals(id, tag.getId().toString());
				}

				return ret;
			}
		};

		if (predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForTagExternalId(final String externalId, List<Predicate> predicates) {
		if (StringUtils.isEmpty(externalId)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {

				boolean ret = false;

				if (object == null) {
					return ret;
				}

				if (object instanceof RangerTag) {
					RangerTag tag = (RangerTag) object;

					ret = StringUtils.equals(externalId, tag.getGuid());
				}

				return ret;
			}
		};

		if (predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForTagName(final String name, List<Predicate> predicates) {
		if (StringUtils.isEmpty(name)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {

				boolean ret = false;

				if (object == null) {
					return ret;
				}

				if (object instanceof RangerTag) {
					RangerTag tag = (RangerTag) object;

					ret = StringUtils.equals(name, tag.getName());
				}

				return ret;
			}
		};

		if (predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForResourceId(final String id, List<Predicate> predicates) {
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

				if (object instanceof RangerServiceResource) {
					RangerServiceResource resource = (RangerServiceResource) object;

					ret = StringUtils.equals(id, resource.getId().toString());
				}

				return ret;
			}
		};

		if (predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForResourceExternalId(final String id, List<Predicate> predicates) {
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

				if (object instanceof RangerServiceResource) {
					RangerServiceResource resource = (RangerServiceResource) object;

					ret = StringUtils.equals(id, resource.getGuid());
				}

				return ret;
			}
		};

		if (predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForServiceResourceServiceName(final String serviceName, List<Predicate> predicates) {
		if (serviceName == null || StringUtils.isEmpty(serviceName)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {

				boolean ret = false;

				if (object == null) {
					return ret;
				}

				if (object instanceof RangerServiceResource) {
					RangerServiceResource resource = (RangerServiceResource) object;
					ret = StringUtils.equals(resource.getServiceName(), serviceName);
				}

				return ret;
			}
		};

		if (predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForResourceSignature(final String signature, List<Predicate> predicates) {
		if (StringUtils.isEmpty(signature)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {

				boolean ret = false;

				if (object == null) {
					return ret;
				}

				if (object instanceof RangerServiceResource) {
					RangerServiceResource resource = (RangerServiceResource) object;

					ret = StringUtils.equals(signature, resource.getResourceSignature());
				}

				return ret;
			}
		};

		if (predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForTagResourceMapId(final String id, List<Predicate> predicates) {
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

				if (object instanceof RangerTagResourceMap) {
					RangerTagResourceMap tagResourceMap = (RangerTagResourceMap) object;
					ret = StringUtils.equals(id, tagResourceMap.getId().toString());
				}

				return ret;
			}
		};

		if (predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForTagResourceMapResourceId(final String resourceId, List<Predicate> predicates) {
		if (StringUtils.isEmpty(resourceId)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {

				boolean ret = false;

				if (object == null) {
					return ret;
				}

				if (object instanceof RangerTagResourceMap) {
					RangerTagResourceMap tagResourceMap = (RangerTagResourceMap) object;
					ret = StringUtils.equals(resourceId, tagResourceMap.getResourceId().toString());
				}

				return ret;
			}
		};

		if (predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForTagResourceMapTagId(final String tagId, List<Predicate> predicates) {
		if (StringUtils.isEmpty(tagId)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {

				boolean ret = false;

				if (object == null) {
					return ret;
				}

				if (object instanceof RangerTagResourceMap) {
					RangerTagResourceMap tagResourceMap = (RangerTagResourceMap) object;
					ret = StringUtils.equals(tagId, tagResourceMap.getTagId().toString());
				}

				return ret;
			}
		};

		if (predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}
}
