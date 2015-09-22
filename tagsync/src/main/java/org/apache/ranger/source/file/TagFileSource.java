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

package org.apache.ranger.source.file;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.model.TagSink;
import org.apache.ranger.model.TagSource;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.model.RangerTagResourceMap;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.process.TagSyncConfig;

import java.io.*;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by akulkarni on 9/11/15.
 */
public class TagFileSource implements TagSource, Runnable {
	private static final Log LOG = LogFactory.getLog(TagFileSource.class);

	public static final String CREATE_OR_UPDATE_SERVICETAGS_OP = "CREATE_OR_UPDATE";
	public static final String DELETE_SERVICETAGS_OP = "DELETE";

	private String sourceFileName;
	private long lastModifiedTimeInMillis = 0L;

	private Gson gson;
	private TagSink tagSink;
	private Properties properties;

	@Override
	public boolean initialize(Properties properties) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileSource.initialize()");
		}

		if (properties == null || MapUtils.isEmpty(properties)) {
			LOG.error("No properties specified for TagFileSource initialization");
			this.properties = new Properties();
		} else {
			this.properties = properties;
		}

		boolean ret = true;

		if (ret) {

			sourceFileName = TagSyncConfig.getTagSourceFileName(properties);

			if (LOG.isDebugEnabled()) {
				LOG.debug("Provided sourceFileName=" + sourceFileName);
			}

			String realFileName = TagSyncConfig.getResourceFileName(sourceFileName);
			if (realFileName != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Real sourceFileName=" + realFileName);
				}
				sourceFileName = realFileName;
			} else {
				LOG.error(sourceFileName + " is not a file or is not readable");
				ret = false;
			}
		}

		if (ret) {
			try {
				gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
			} catch (Throwable excp) {
				LOG.fatal("failed to create GsonBuilder object", excp);
				ret = false;
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileSource.initialize(): sourceFileName=" + sourceFileName + ", result=" + ret);
		}

		return ret;
	}

	@Override
	public void setTagSink(TagSink sink) {
		if (sink == null) {
			LOG.error("Sink is null!!!");
		} else {
			this.tagSink = sink;
		}
	}

	@Override
	public void start() {
		(new Thread(this)).start();
	}

	@Override
	public void run() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileSource.run()");
		}
		long sleepTimeBetweenCycleInMillis = TagSyncConfig.getSleepTimeInMillisBetweenCycle(properties);
		boolean shutdownFlag = false;

		while (!shutdownFlag) {

			try {
				if (isChanged()) {
					LOG.info("Begin: update tags from source==>sink");
					if (TagSyncConfig.isTagSyncEnabled(properties)) {
						updateSink();
						LOG.info("End: update tags from source==>sink");
					} else {
						LOG.info("Tag-sync is not enabled.");
					}
				} else {
					LOG.debug("TagFileSource: no change found for synchronization.");
				}

				LOG.debug("Sleeping for [" + sleepTimeBetweenCycleInMillis + "] milliSeconds");

				Thread.sleep(sleepTimeBetweenCycleInMillis);
			}
			catch (InterruptedException e) {
				LOG.error("Failed to wait for [" + sleepTimeBetweenCycleInMillis + "] milliseconds before attempting to synchronize tag information", e);
				shutdownFlag = true;
			}
			catch (Throwable t) {
				LOG.error("tag-sync thread got an error", t);
				shutdownFlag = true;
			}

		}

		LOG.error("Shutting down the Tag-file-source thread");

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileSource.run()");
		}
	}

	@Override
	public void updateSink() throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileSource.updateSink()");
		}
		ServiceTags serviceTags = readFromFile();

		if (serviceTags != null) {

			tagSink.uploadServiceTags(serviceTags);

		} else {
			LOG.error("Could not read ServiceTags from file");
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileSource.updateSink()");
		}
	}

	/*
	private void createTagObjects(ServiceTags serviceTags) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileSource.createTagObjects()");
		}

		Map<Long, RangerTagDef> tagDefsMap = serviceTags.getTagDefinitions();
		if (MapUtils.isNotEmpty(tagDefsMap)) {
			for (Map.Entry<Long, RangerTagDef> entry : tagDefsMap.entrySet()) {
				RangerTagDef tagDef = entry.getValue();
				try {
					tagSink.createTagDef(tagDef);
				} catch (Exception exception) {
					// Ignore and continue
					LOG.error("createTagDef failed, tagDef=" + tagDef, exception);
				}
			}
		}

		List<RangerServiceResource> serviceResources = serviceTags.getServiceResources();
		if (CollectionUtils.isNotEmpty(serviceResources)) {
			for (RangerServiceResource serviceResource : serviceResources) {
				try {
					tagSink.createServiceResource(serviceResource);
				} catch (Exception exception) {
					// Ignore and continue
					LOG.error("createServiceResource failed, serviceResource=" + serviceResource, exception);
				}
			}
		}

		Map<Long, RangerTag> tagsMap = serviceTags.getTags();
		if (MapUtils.isNotEmpty(tagsMap)) {
			for (Map.Entry<Long, RangerTag> entry : tagsMap.entrySet()) {
				RangerTag tag = entry.getValue();
				try {
					tagSink.createTag(tag);
				} catch (Exception exception) {
					// Ignore and continue
					LOG.error("createTag failed, tag=" + tag, exception);
				}
			}
		}

		Map<Long, List<Long>> resourceTagIdsMap = serviceTags.getResourceToTagIds();
		if (MapUtils.isNotEmpty(resourceTagIdsMap)) {
			for (Map.Entry<Long, List<Long>> entry : resourceTagIdsMap.entrySet()) {
				Long resourceId = entry.getKey();
				List<Long> tagIds = entry.getValue();
				for (Long tagId : tagIds) {
					RangerTagResourceMap tagResourceMap = new RangerTagResourceMap();
					tagResourceMap.setResourceId(resourceId);
					tagResourceMap.setTagId(tagId);
					try {
						tagSink.createTagResourceMap(tagResourceMap);
					} catch (Exception exception) {
						LOG.error("createTagResourceMap failed, resourceId=" + resourceId + ", tagId=" + tagId, exception);
					}
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileSource.createTagObjects()");
		}
	}
	*/
	/*
	private void deleteTagObjects(ServiceTags serviceTags) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileSource.deleteTagObjects()");
		}

		Map<Long, List<Long>> resourceTagIdsMap = serviceTags.getResourceToTagIds();
		if (MapUtils.isNotEmpty(resourceTagIdsMap)) {
			for (Map.Entry<Long, List<Long>> entry : resourceTagIdsMap.entrySet()) {
				Long resourceId = entry.getKey();
				List<Long> tagIds = entry.getValue();
				for (Long tagId : tagIds) {
					try {
						tagSink.deleteTagResourceMap(tagId, resourceId);
					} catch (Exception exception) {
						LOG.error("deleteTagResourceMap failed, resourceId=" + resourceId + ", tagId=" + tagId +")", exception);
					}
				}
			}
		}

		List<RangerServiceResource> serviceResources = serviceTags.getServiceResources();
		if (CollectionUtils.isNotEmpty(serviceResources)) {
			for (RangerServiceResource serviceResource : serviceResources) {
				try {
					tagSink.deleteServiceResource(serviceResource.getId());
				} catch (Exception exception) {
					// Ignore and continue
					LOG.error("deleteServiceResource failed, serviceResource=" + serviceResource, exception);
				}
			}
		}

		Map<Long, RangerTag> tagsMap = serviceTags.getTags();
		if (MapUtils.isNotEmpty(tagsMap)) {
			for (Map.Entry<Long, RangerTag> entry : tagsMap.entrySet()) {
				Long tagId = entry.getKey();
				try {
					tagSink.deleteTag(tagId);
				} catch (Exception exception) {
					// Ignore and continue
					LOG.error("deleteTag failed, tagId=" + tagId, exception);
				}
			}
		}

		Map<Long, RangerTagDef> tagDefsMap = serviceTags.getTagDefinitions();
		if (MapUtils.isNotEmpty(tagDefsMap)) {
			for (Map.Entry<Long, RangerTagDef> entry : tagDefsMap.entrySet()) {
				Long tagDefId = entry.getKey();
				try {
					tagSink.deleteTagDef(tagDefId);
				} catch (Exception exception) {
					// Ignore and continue
					LOG.error("deleteTagDef failed, tagDefId=" + tagDefId, exception);
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileSource.deleteTagObjects()");
		}
	}
	*/

	@Override
	public 	boolean isChanged() {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileSource.isChanged()");
		}
		boolean ret = false;

		long modificationTime = getModificationTime();

		if (modificationTime > lastModifiedTimeInMillis) {
			if (LOG.isDebugEnabled()) {
				Date modifiedDate = new Date(modificationTime);
				Date lastModifiedDate = new Date(lastModifiedTimeInMillis);
				LOG.debug("File modified at " + modifiedDate + "last-modified at " + lastModifiedDate);
			}
			lastModifiedTimeInMillis = modificationTime;
			ret = true;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileSource.isChanged(): result=" + ret);
		}
		return ret;
	}

	@Override
	public List<RangerTagDef> fetchAllTagDefs(String syncSentinel) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTagDef> receiveUpdatesToTagDefs() throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTagResourceMap> fetchAllTaggedEntities() throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTagResourceMap> receiveUpdatesToTaggedEntities() throws Exception {
		throw new Exception("Not implemented");
	}

	private ServiceTags readFromFile() {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileSource.readFromFile(): sourceFileName=" + sourceFileName);
		}

		ServiceTags ret = null;

		Reader reader = null;
		try {

			reader = new InputStreamReader(TagSyncConfig.getFileInputStream(sourceFileName));

			ret = gson.fromJson(reader, ServiceTags.class);

		}
		catch (FileNotFoundException exception) {
			LOG.warn("Tag-source file does not exist or not readble '" + sourceFileName + "'");
		}
		catch (Exception excp) {
			LOG.error("failed to load service-tags from Tag-source file " + sourceFileName, excp);
		}
		finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception excp) {
					LOG.error("error while closing opened Tag-source file " + sourceFileName, excp);
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileSource.readFromFile(): sourceFileName=" + sourceFileName);
		}

		return ret;
	}

	private long getModificationTime() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileSource.getLastModificationTime(): sourceFileName=" + sourceFileName);
		}
		long ret = 0L;

		File sourceFile = new File(sourceFileName);

		if (sourceFile.exists() && sourceFile.isFile() && sourceFile.canRead()) {
			ret = sourceFile.lastModified();
		} else {
			ret = new Date().getTime();
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileSource.lastModificationTime(): sourceFileName=" + sourceFileName + " result=" + new Date(ret));
		}

		return ret;
	}

}
