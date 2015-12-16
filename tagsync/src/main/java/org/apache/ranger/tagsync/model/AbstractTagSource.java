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

package org.apache.ranger.tagsync.model;

import com.google.gson.Gson;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.util.ServiceTags;

public abstract  class AbstractTagSource implements TagSource {
	private static final Log LOG = LogFactory.getLog(AbstractTagSource.class);
	private TagSink tagSink;
	protected boolean shutdown = false;

	@Override
	public void setTagSink(TagSink sink) {
		if (sink == null) {
			LOG.error("Sink is null!!!");
		} else {
			this.tagSink = sink;
		}
	}
	@Override
	public void synchUp() {}

	public void updateSink(final ServiceTags serviceTags) {
		if (serviceTags == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No ServiceTags to upload");
			}
		} else {
			if (LOG.isDebugEnabled()) {
				String serviceTagsJSON = new Gson().toJson(serviceTags);
				LOG.debug("Uploading serviceTags=" + serviceTagsJSON);
			}

			try {
				tagSink.uploadServiceTags(serviceTags);
			} catch (Exception exception) {
				LOG.error("uploadServiceTags() failed..", exception);
			}
		}
	}

	public void stop() {
		shutdown = true;
	}
}
