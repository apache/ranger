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

package org.apache.ranger.plugin.store.file;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.*;
import org.apache.ranger.plugin.store.AbstractTagStore;
import org.apache.ranger.plugin.store.TagPredicateUtil;
import org.apache.ranger.plugin.store.TagStore;
import org.apache.ranger.plugin.util.SearchFilter;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TagFileStore extends AbstractTagStore {
	private static final Log LOG = LogFactory.getLog(TagFileStore.class);

	public static final String PROPERTY_TAG_FILE_STORE_DIR = "ranger.tag.store.file.dir";
	protected static final String FILE_PREFIX_TAG_DEF = "ranger-tagdef-";
	protected static final String FILE_PREFIX_TAG = "ranger-tag-";
	protected static final String FILE_PREFIX_RESOURCE = "ranger-serviceresource-";
	protected static final String FILE_PREFIX_TAG_RESOURCE_MAP = "ranger-tagresourcemap-";

	private String tagDataDir = null;
	private long nextTagDefId = 0;
	private long nextTagId = 0;
	private long nextServiceResourceId = 0;
	private long nextTagResourceMapId = 0;


	private TagPredicateUtil predicateUtil = null;
	private FileStoreUtil fileStoreUtil = null;

	private volatile static TagFileStore instance = null;

	public static TagStore getInstance() {
		if (instance == null) {
			synchronized (TagFileStore.class) {
				if (instance == null) {
					instance = new TagFileStore();
					instance.initStore();
				}
			}
		}
		return instance;
	}

	TagFileStore() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.TagFileStore()");
		}

		tagDataDir = RangerConfiguration.getInstance().get(PROPERTY_TAG_FILE_STORE_DIR, "file:///etc/ranger/data");
		fileStoreUtil = new FileStoreUtil();
		predicateUtil = new TagPredicateUtil();

		if (LOG.isDebugEnabled())

		{
			LOG.debug("<== TagFileStore.TagFileStore()");
		}
	}

	@Override
	public void init() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.init()");
		}

		super.init();
		fileStoreUtil.initStore(tagDataDir);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.init()");
		}
	}

	protected void initStore() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.initStore()");
		}
		fileStoreUtil.initStore(tagDataDir);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.initStore()");
		}
	}

	@Override
	public RangerTagDef createTagDef(RangerTagDef tagDef) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.createTagDef(" + tagDef + ")");
		}

		List<RangerTagDef> existing = getTagDef(tagDef.getName());

		if (CollectionUtils.isNotEmpty(existing)) {
			throw new Exception(tagDef.getName() + ": tag-def already exists (id=" + existing.get(0).getId() + ")");
		}

		RangerTagDef ret;

		try {
			preCreate(tagDef);

			tagDef.setId(nextTagDefId);

			ret = fileStoreUtil.saveToFile(tagDef, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG_DEF, nextTagDefId++)), false);

			postCreate(ret);
		} catch (Exception excp) {
			LOG.warn("TagFileStore.createTagDef(): failed to save tag-def '" + tagDef.getName() + "'", excp);

			throw new Exception("failed to save tag-def '" + tagDef.getName() + "'", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.createTagDef(" + tagDef + ")");
		}

		return ret;
	}

	@Override
	public RangerTagDef updateTagDef(RangerTagDef tagDef) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.updateTagDef(" + tagDef + ")");
		}

		RangerTagDef existing = null;

		if(tagDef.getId() == null) {
			List<RangerTagDef> existingDefs = getTagDef(tagDef.getName());

			if (CollectionUtils.isEmpty(existingDefs)) {
				throw new Exception("tag-def does not exist: name=" + tagDef.getName());
			}
		} else {
			existing = this.getTagDefById(tagDef.getId());

			if (existing == null) {
				throw new Exception("tag-def does not exist: id=" + tagDef.getId());
			}
		}


		RangerTagDef ret;

		try {
			preUpdate(existing);

			existing.setSource(tagDef.getSource());
			existing.setAttributeDefs(tagDef.getAttributeDefs());

			ret = fileStoreUtil.saveToFile(existing, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG_DEF, existing.getId())), true);

			postUpdate(existing);
		} catch (Exception excp) {
			LOG.warn("TagFileStore.updateTagDef(): failed to save tag-def '" + tagDef.getName() + "'", excp);

			throw new Exception("failed to save tag-def '" + tagDef.getName() + "'", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.updateTagDef(" + tagDef + ")");
		}

		return ret;
	}

	@Override
	public void deleteTagDef(String name) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.deleteTagDef(" + name + ")");
		}

		List<RangerTagDef> existingDefs = getTagDef(name);

		if (CollectionUtils.isEmpty(existingDefs)) {
			throw new Exception("no tag-def exists with name=" + name);
		}

		try {
			for(RangerTagDef existing : existingDefs) {
				Path filePath = new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG_DEF, existing.getId()));

				preDelete(existing);

				fileStoreUtil.deleteFile(filePath);

				postDelete(existing);
			}
		} catch (Exception excp) {
			throw new Exception("failed to delete tag-def with ID=" + name, excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.deleteTagDef(" + name + ")");
		}

	}

	@Override
	public List<RangerTagDef> getTagDef(String name) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagDef(" + name + ")");
		}

		List<RangerTagDef> ret;

		if (StringUtils.isNotBlank(name)) {
			SearchFilter filter = new SearchFilter(SearchFilter.TAG_DEF_NAME, name);

			List<RangerTagDef> tagDefs = getTagDefs(filter);

			ret = CollectionUtils.isEmpty(tagDefs) ? null : tagDefs;
		} else {
			ret = null;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagDef(" + name + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerTagDef getTagDefById(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagDefById(" + id + ")");
		}

		RangerTagDef ret;

		if (id != null) {
			SearchFilter filter = new SearchFilter(SearchFilter.TAG_DEF_ID, id.toString());

			List<RangerTagDef> tagDefs = getTagDefs(filter);

			ret = CollectionUtils.isEmpty(tagDefs) ? null : tagDefs.get(0);
		} else {
			ret = null;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagDefById(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerTagDef> getTagDefs(SearchFilter filter) throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagDefs()");
		}

		List<RangerTagDef> ret = getAllTagDefs();

		if (CollectionUtils.isNotEmpty(ret) && filter != null && !filter.isEmpty()) {
			CollectionUtils.filter(ret, predicateUtil.getPredicate(filter));

			//Comparator<RangerBaseModelObject> comparator = getSorter(filter);

			//if(comparator != null) {
			//Collections.sort(ret, comparator);
			//}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagDefs(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public RangerTag createTag(RangerTag tag) throws Exception
	{
		RangerTag ret;

		try {
			preCreate(tag);

			tag.setId(nextTagId);

			ret = fileStoreUtil.saveToFile(tag, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG, nextTagId++)), false);

			postCreate(ret);
		} catch (Exception excp) {
			LOG.warn("TagFileStore.createTag(): failed to save tag '" + tag.getName() + "'", excp);

			throw new Exception("failed to save tag '" + tag.getName() + "'", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.createTag(" + tag + ")");
		}

		return ret;
	}

	@Override
	public RangerTag updateTag(RangerTag tag) throws Exception
	{
		RangerTag ret;

		try {
			preUpdate(tag);

			ret = fileStoreUtil.saveToFile(tag, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG, tag.getId())), true);

			postUpdate(tag);
		} catch (Exception excp) {
			LOG.warn("TagFileStore.updateTag(): failed to save tag '" + tag.getName() + "'", excp);

			throw new Exception("failed to save tag '" + tag.getName() + "'", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.updateTag(" + tag + ")");
		}

		return ret;
	}

	@Override
	public void deleteTagById(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.deleteTag(" + id + ")");
		}

		try {
			RangerTag tag = getTagById(id);

			Path filePath = new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG, tag.getId()));

			preDelete(tag);

			fileStoreUtil.deleteFile(filePath);

			postDelete(tag);

		} catch (Exception excp) {
			throw new Exception("failed to delete tag with ID=" + id, excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.deleteTag(" + id + ")");
		}
	}

	@Override
	public List<RangerTag> getTags(SearchFilter filter) throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTags()");
		}

		List<RangerTag> ret = getAllTags();

		if (CollectionUtils.isNotEmpty(ret) && filter != null && !filter.isEmpty()) {
			CollectionUtils.filter(ret, predicateUtil.getPredicate(filter));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTags(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}


	@Override
	public RangerServiceResource createServiceResource(RangerServiceResource resource) throws Exception {
		RangerServiceResource ret;

		try {
			preCreate(resource);

			resource.setId(nextServiceResourceId);

			ret = fileStoreUtil.saveToFile(resource, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_RESOURCE, nextServiceResourceId++)), false);

			postCreate(ret);
		} catch (Exception excp) {
			LOG.warn("TagFileStore.createServiceResource(): failed to save resource '" + resource.getId() + "'", excp);

			throw new Exception("failed to save service-resource '" + resource.getId() + "'", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.createServiceResource(" + resource + ")");
		}

		return ret;
	}

	@Override
	public RangerServiceResource updateServiceResource(RangerServiceResource resource) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.updateServiceResource(" + resource + ")");
		}
		RangerServiceResource ret;

		try {
			preUpdate(resource);

			ret = fileStoreUtil.saveToFile(resource, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_RESOURCE, resource.getId())), true);

			postUpdate(resource);
		} catch (Exception excp) {
			LOG.warn("TagFileStore.updateServiceResource(): failed to save resource '" + resource.getId() + "'", excp);

			throw new Exception("failed to save service-resource '" + resource.getId() + "'", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.updateServiceResource(" + resource + ")");
		}

		return ret;

	}

	@Override
	public void deleteServiceResourceById(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.deleteServiceResource(" + id + ")");
		}

		try {
			RangerServiceResource resource = getServiceResourceById(id);

			Path filePath = new Path(fileStoreUtil.getDataFile(FILE_PREFIX_RESOURCE, resource.getId()));

			preDelete(resource);

			fileStoreUtil.deleteFile(filePath);

			postDelete(resource);

		} catch (Exception excp) {
			throw new Exception("failed to delete service-resource with ID=" + id, excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.deleteServiceResource(" + id + ")");
		}
	}

	@Override
	public List<RangerServiceResource> getServiceResources(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getServiceResources()");
		}

		List<RangerServiceResource> ret = getAllResources();

		if (CollectionUtils.isNotEmpty(ret) && filter != null && !filter.isEmpty()) {
			CollectionUtils.filter(ret, predicateUtil.getPredicate(filter));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getServicesResources(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public RangerTagResourceMap createTagResourceMap(RangerTagResourceMap tagResourceMap) throws Exception {

		RangerTagResourceMap ret;

		preCreate(tagResourceMap);

		tagResourceMap.setId(nextTagResourceMapId);

		ret = fileStoreUtil.saveToFile(tagResourceMap, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG_RESOURCE_MAP, nextTagResourceMapId++)), false);

		postCreate(ret);

		return ret;
	}

	@Override
	public void deleteTagResourceMapById(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.deleteTagResourceMapById(" + id + ")");
		}

		try {
			RangerTagResourceMap tagResourceMap = getTagResourceMapById(id);

			Path filePath = new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG_RESOURCE_MAP, tagResourceMap.getId()));

			preDelete(tagResourceMap);

			fileStoreUtil.deleteFile(filePath);

			postDelete(tagResourceMap);

		} catch (Exception excp) {
			throw new Exception("failed to delete tagResourceMap with ID=" + id, excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.deleteTagResourceMapById(" + id + ")");
		}
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMaps(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagResourceMaps()");
		}

		List<RangerTagResourceMap> ret = getAllTaggedResources();

		if (CollectionUtils.isNotEmpty(ret) && filter != null && !filter.isEmpty()) {
			CollectionUtils.filter(ret, predicateUtil.getPredicate(filter));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagResourceMaps(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;

	}

	private List<RangerTagDef> getAllTagDefs() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getAllTagDefs()");
		}

		List<RangerTagDef> ret = new ArrayList<RangerTagDef>();

		try {
			// load Tag definitions from file system
			List<RangerTagDef> sds = fileStoreUtil.loadFromDir(new Path(fileStoreUtil.getDataDir()), FILE_PREFIX_TAG_DEF, RangerTagDef.class);

			if (CollectionUtils.isNotEmpty(sds)) {
				for (RangerTagDef sd : sds) {
					if (sd != null) {
						// if the TagDef is already found, remove the earlier definition
						for (int i = 0; i < ret.size(); i++) {
							RangerTagDef currSd = ret.get(i);

							if (StringUtils.equals(currSd.getName(), sd.getName()) ||
									ObjectUtils.equals(currSd.getId(), sd.getId())) {
								ret.remove(i);
							}
						}

						ret.add(sd);
					}
				}
			}
			nextTagDefId = getMaxId(ret) + 1;
		} catch (Exception excp) {
			LOG.error("TagFileStore.getAllTagDefs(): failed to read Tag-defs", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getAllTagDefs(): count=" + ret.size());
		}

		return ret;
	}

	private List<RangerTag> getAllTags() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getAllTags()");
		}

		List<RangerTag> ret = new ArrayList<RangerTag>();

		try {
			List<RangerTag> sds = fileStoreUtil.loadFromDir(new Path(fileStoreUtil.getDataDir()), FILE_PREFIX_TAG, RangerTag.class);

			if (CollectionUtils.isNotEmpty(sds)) {
				for (RangerTag sd : sds) {
					if (sd != null) {
						// if the Tag is already found, remove the earlier one
						for (int i = 0; i < ret.size(); i++) {
							RangerTag currSd = ret.get(i);

							if (StringUtils.equals(currSd.getName(), sd.getName()) ||
									ObjectUtils.equals(currSd.getId(), sd.getId())) {
								ret.remove(i);
							}
						}

						ret.add(sd);
					}
				}
			}
			nextTagId = getMaxId(ret) + 1;
		} catch (Exception excp) {
			LOG.error("TagFileStore.getAllTags(): failed to read Tags", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getAllTags(): count=" + ret.size());
		}

		return ret;
	}

	private List<RangerServiceResource> getAllResources() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getAllResources()");
		}

		List<RangerServiceResource> ret = new ArrayList<RangerServiceResource>();

		try {
			List<RangerServiceResource> sds = fileStoreUtil.loadFromDir(new Path(fileStoreUtil.getDataDir()), FILE_PREFIX_RESOURCE, RangerServiceResource.class);

			if (CollectionUtils.isNotEmpty(sds)) {
				for (RangerServiceResource sd : sds) {
					if (sd != null) {
						// if the resource is already found, remove the earlier one
						for (int i = 0; i < ret.size(); i++) {
							RangerServiceResource currSd = ret.get(i);

							if (ObjectUtils.equals(currSd.getId(), sd.getId())) {
								ret.remove(i);
							}
						}

						ret.add(sd);
					}
				}
			}
			nextServiceResourceId = getMaxId(ret) + 1;
		} catch (Exception excp) {
			LOG.error("TagFileStore.getAllResources(): failed to read Resources", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getAllResourcess(): count=" + ret.size());
		}

		return ret;
	}

	private List<RangerTagResourceMap> getAllTaggedResources() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getAllTaggedResources()");
		}

		List<RangerTagResourceMap> ret = new ArrayList<RangerTagResourceMap>();

		try {
			// load resource definitions from file system
			List<RangerTagResourceMap> resources = fileStoreUtil.loadFromDir(new Path(fileStoreUtil.getDataDir()), FILE_PREFIX_TAG_RESOURCE_MAP, RangerTagResourceMap.class);

			if (CollectionUtils.isNotEmpty(resources)) {
				for (RangerTagResourceMap resource : resources) {
					if (resource != null) {
						// if the RangerTagResourceMap is already found, remove the earlier definition
						for (int i = 0; i < ret.size(); i++) {
							RangerTagResourceMap currResource = ret.get(i);

							if (ObjectUtils.equals(currResource.getId(), resource.getId())) {
								ret.remove(i);
							}
						}

						ret.add(resource);
					}
				}
			}
			nextTagResourceMapId = getMaxId(ret) + 1;
		} catch (Exception excp) {
			LOG.error("TagFileStore.getAllTaggedResources(): failed to read tagged resources", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getAllTaggedResources(): count=" + ret.size());
		}

		return ret;
	}

	@Override
	public List<String> getTags(String serviceName) throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTags(" + serviceName + ")");
		}

		// Ignore serviceName
		List<RangerTag> allTags = getAllTags();

		List<String> ret = new ArrayList<String>();

		for (RangerTag tag : allTags) {
			ret.add(tag.getName());
		}

		return ret;
	}

	@Override
	public List<String> lookupTags(String serviceName, String tagNamePattern) throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.lookupTags(" + serviceName + ", " + tagNamePattern + ")");
		}

		List<String> tagNameList = getTags(serviceName);
		List<String> matchedTagList = new ArrayList<String>();

		if (CollectionUtils.isNotEmpty(tagNameList)) {
			Pattern p = Pattern.compile(tagNamePattern);
			for (String tagName : tagNameList) {
				Matcher m = p.matcher(tagName);
				if (LOG.isDebugEnabled()) {
					LOG.debug("TagFileStore.lookupTags) - Trying to match .... tagNamePattern=" + tagNamePattern + ", tagName=" + tagName);
				}
				if (m.matches()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("TagFileStore.lookupTags) - Match found.... tagNamePattern=" + tagNamePattern + ", tagName=" + tagName);
					}
					matchedTagList.add(tagName);
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.lookupTags(" + serviceName + ", " + tagNamePattern + ")");
		}

		return matchedTagList;
	}

	@Override
	public void deleteTagDefById(Long id) throws Exception {
		// TODO Auto-generated method stub

	}
}

