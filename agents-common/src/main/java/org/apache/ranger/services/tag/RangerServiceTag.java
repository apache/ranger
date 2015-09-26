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

package org.apache.ranger.services.tag;

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.store.TagStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RangerServiceTag extends RangerBaseService {

	private static final Log LOG = LogFactory.getLog(RangerServiceTag.class);

	public static final String TAG_RESOURCE_NAME = "tag";

	private TagStore tagStore = null;


	public RangerServiceTag() {
		super();
	}

	@Override
	public void init(RangerServiceDef serviceDef, RangerService service) {
		super.init(serviceDef, service);
	}

	public void setTagStore(TagStore tagStore) {
		this.tagStore = tagStore;
	}

	@Override
	public HashMap<String,Object> validateConfig() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceTag.validateConfig(" + serviceName + " )");
		}

		HashMap<String, Object> ret = new HashMap<String, Object>();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceTag.validateConfig(" + serviceName + " ): " + ret);
		}

		return ret;
	}

	@Override
	public List<String> lookupResource(ResourceLookupContext context) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceTag.lookupResource(" + context + ")");
		}

		List<String> ret = new ArrayList<String>();

		if (context != null && StringUtils.equals(context.getResourceName(), TAG_RESOURCE_NAME)) {
			try {
				List<String> tags = tagStore != null ? tagStore.getTagTypes() : null;

				if(CollectionUtils.isNotEmpty(tags)) {
					List<String> valuesToExclude = MapUtils.isNotEmpty(context.getResources()) ? context.getResources().get(TAG_RESOURCE_NAME) : null;

					if(CollectionUtils.isNotEmpty(valuesToExclude)) {
						for (String valueToExclude : valuesToExclude) {
							tags.remove(valueToExclude);
						}
					}

					String valueToMatch = context.getUserInput();

					if(StringUtils.isNotEmpty(valueToMatch)) {
						if(! valueToMatch.endsWith("*")) {
							valueToMatch += "*";
						}

						for (String tag : tags) {
							if(FilenameUtils.wildcardMatch(tag, valueToMatch)) {
								ret.add(tag);
							}
						}
					}
				}
			} catch (Exception excp) {
				LOG.error("RangerServiceTag.lookupResource()", excp);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceTag.lookupResource(): tag count=" + ret.size());
		}

		return ret;
	}
}
