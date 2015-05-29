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
import org.apache.ranger.plugin.model.RangerResource;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.store.AbstractTagStore;
import org.apache.ranger.plugin.store.TagPredicateUtil;
import org.apache.ranger.plugin.util.SearchFilter;

import java.util.ArrayList;
import java.util.List;

public class TagFileStore extends AbstractTagStore {
	private static final Log LOG = LogFactory.getLog(TagFileStore.class);

	public static final String PROPERTY_TAG_FILE_STORE_DIR = "ranger.tag.store.file.dir";
	protected static final String FILE_PREFIX_TAG_DEF = "ranger-tagdef-";
	protected static final String FILE_PREFIX_TAG_RESOURCE = "ranger-tag-resource-";

	private String tagDataDir = null;
	private long nextTagDefId = 0;
	private long nextTagResourceId = 0;


	private TagPredicateUtil predicateUtil = null;
	private FileStoreUtil fileStoreUtil = null;

	private volatile static TagFileStore instance = null;

	public static TagFileStore getInstance() {
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
		predicateUtil = new TagPredicateUtil(this);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.initStore()");
		}
	}

	@Override
	public RangerTagDef createTagDef(RangerTagDef tagDef) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.createTagDef(" + tagDef + ")");
		}

		RangerTagDef existing = getTagDef(tagDef.getName());

		if (existing != null) {
			throw new Exception(tagDef.getName() + ": tag-def already exists (id=" + existing.getId() + ")");
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

		RangerTagDef existing = getTagDef(tagDef.getName());

		if (existing == null) {
			throw new Exception(tagDef.getName() + ": tag-def does not exist (id=" + tagDef.getId() + ")");
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

		RangerTagDef existing = getTagDef(name);

		if (existing == null) {
			throw new Exception("no tag-def exists with ID=" + name);
		}

		try {
			Path filePath = new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG_DEF, existing.getId()));

			preDelete(existing);

			fileStoreUtil.deleteFile(filePath);

			postDelete(existing);
		} catch (Exception excp) {
			throw new Exception("failed to delete tag-def with ID=" + name, excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.deleteTagDef(" + name + ")");
		}

	}

	@Override
	public RangerTagDef getTagDef(String name) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagDef(" + name + ")");
		}

		RangerTagDef ret;

		if (StringUtils.isNotBlank(name)) {
			SearchFilter filter = new SearchFilter(SearchFilter.TAG_DEF_NAME, name);

			List<RangerTagDef> tagDefs = getTagDefs(filter);

			ret = CollectionUtils.isEmpty(tagDefs) ? null : tagDefs.get(0);
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
	public RangerResource createResource(RangerResource resource) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.createResource(" + resource + ")");
		}

		RangerResource existing = null;
		if (resource.getId() != null) {
			existing = getResource(resource.getId());
		}

		if (existing != null) {
			throw new Exception(resource.getId() + ": resource already exists (id=" + existing.getId() + ")");
		}

		RangerResource ret;

		try {
			preCreate(resource);

			resource.setId(nextTagResourceId);

			ret = fileStoreUtil.saveToFile(resource, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG_RESOURCE, nextTagResourceId++)), false);

			postCreate(ret);
		} catch (Exception excp) {
			LOG.warn("TagFileStore.createResource(): failed to save resource '" + resource.getId() + "'", excp);

			throw new Exception("failed to save resource '" + resource.getId() + "'", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.createResource(" + resource + ")");
		}

		return ret;
	}

	@Override
	public RangerResource updateResource(RangerResource resource) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.updateResource(" + resource + ")");
		}
		RangerResource existing = getResource(resource.getId());

		if (existing == null) {
			throw new Exception(resource.getId() + ": resource does not exist (id=" + resource.getId() + ")");
		}

		RangerResource ret;

		try {
			preUpdate(existing);

			existing.setComponentType(resource.getComponentType());
			existing.setResourceSpec(resource.getResourceSpec());
			existing.setTagServiceName(resource.getTagServiceName());
			existing.setTags(resource.getTags());

			ret = fileStoreUtil.saveToFile(existing, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG_RESOURCE, existing.getId())), true);

			postUpdate(existing);
		} catch (Exception excp) {
			LOG.warn("TagFileStore.updateTagDef(): failed to save resource '" + resource.getId() + "'", excp);

			throw new Exception("failed to save tag-def '" + resource.getId() + "'", excp);
		}


		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.updateResource(" + resource + ")");
		}
		return ret;
	}

	@Override
	public void deleteResource(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.deleteResource(" + id + ")");
		}

		RangerResource existing = getResource(id);

		if (existing == null) {
			throw new Exception("no resource exists with ID=" + id);
		}

		try {
			Path filePath = new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG_RESOURCE, existing.getId()));

			preDelete(existing);

			fileStoreUtil.deleteFile(filePath);

			postDelete(existing);
		} catch (Exception excp) {
			throw new Exception("failed to delete resource with ID=" + id, excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.deleteResource(" + id + ")");
		}
	}

	@Override
	public RangerResource getResource(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getResource(" + id + ")");
		}
		RangerResource ret;

		if (id != null) {
			SearchFilter filter = new SearchFilter(SearchFilter.TAG_RESOURCE_ID, id.toString());

			List<RangerResource> resources = getResources(filter);

			ret = CollectionUtils.isEmpty(resources) ? null : resources.get(0);
		} else {
			ret = null;
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getResource(" + id + ")");
		}
		return ret;
	}

	@Override
	public List<RangerResource> getResources(String tagServiceName, String componentType) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getResources(" + tagServiceName + ", " + componentType + ")");
		}
		List<RangerResource> ret;

		SearchFilter filter = new SearchFilter();

		if (StringUtils.isNotBlank(tagServiceName)) {
			filter.setParam(SearchFilter.TAG_RESOURCE_SERVICE_NAME, tagServiceName);
		}

		if (StringUtils.isNotBlank(componentType)) {
			filter.setParam(SearchFilter.TAG_RESOURCE_COMPONENT_TYPE, componentType);
		}

		ret = getResources(filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getResources(" + tagServiceName + ", " + componentType + ")");

		}
		return ret;
	}

	@Override
	public List<RangerResource> getResources(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getResources()");
		}

		List<RangerResource> ret = getAllTaggedResources();

		if (CollectionUtils.isNotEmpty(ret) && filter != null && !filter.isEmpty()) {
			CollectionUtils.filter(ret, predicateUtil.getPredicate(filter));

			//Comparator<RangerBaseModelObject> comparator = getSorter(filter);

			//if(comparator != null) {
			//Collections.sort(ret, comparator);
			//}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getResources(): count=" + (ret == null ? 0 : ret.size()));
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

		//Collections.sort(ret, idComparator);

		//for (RangerTagDef sd : ret) {
			//Collections.sort(sd.getResources(), resourceLevelComparator);
		//}

		return ret;
	}

	private List<RangerResource> getAllTaggedResources() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getAllTaggedResources()");
		}

		List<RangerResource> ret = new ArrayList<RangerResource>();

		try {
			// load resource definitions from file system
			List<RangerResource> resources = fileStoreUtil.loadFromDir(new Path(fileStoreUtil.getDataDir()), FILE_PREFIX_TAG_RESOURCE, RangerResource.class);

			if (CollectionUtils.isNotEmpty(resources)) {
				for (RangerResource resource : resources) {
					if (resource != null) {
						// if the RangerResource is already found, remove the earlier definition
						for (int i = 0; i < ret.size(); i++) {
							RangerResource currResource = ret.get(i);

							if (ObjectUtils.equals(currResource.getId(), resource.getId())) {
								ret.remove(i);
							}
						}

						ret.add(resource);
					}
				}
			}
			nextTagResourceId = getMaxId(ret) + 1;
		} catch (Exception excp) {
			LOG.error("TagFileStore.getAllTaggedResources(): failed to read tagged resources", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getAllTaggedResources(): count=" + ret.size());
		}


		//Collections.sort(ret, idComparator);

		//for (RangerTagDef sd : ret) {
			//Collections.sort(sd.getResources(), resourceLevelComparator);
		//}

		return ret;
	}

}

