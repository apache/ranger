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

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.*;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.store.file.ServiceFileStore;
import org.apache.ranger.plugin.store.file.TagFileStore;
import org.apache.ranger.plugin.store.rest.ServiceRESTStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceTags;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestTagStore {
	static TagStore tagStore = null;
	static TagValidator validator = null;

	static SearchFilter filter = null;
	static Path filePath = new Path("file:///etc/ranger/data/ranger-admin-test-site.xml");
	static Configuration config = new Configuration();

	static final String serviceDefJsonFile = "/admin/service-defs/test-hive-servicedef.json";
	static final String serviceName = "tag-unit-test-TestTagStore";

	static final String crcSuffix = ".crc";
	static final String jsonSuffix = ".json";

	static Gson gsonBuilder = null;
	static RangerServiceDef serviceDef = null;
	static RangerService service = null;

	@BeforeClass
	public static void setupTest() throws Exception {

		tearDownAfterClass(crcSuffix);
		tearDownAfterClass(jsonSuffix);

		FileSystem fs = filePath.getFileSystem(config);

		FSDataOutputStream outStream = fs.create(filePath, true);
		OutputStreamWriter writer = null;

		writer = new OutputStreamWriter(outStream);

		writer.write("<configuration>\n" +
				"        <property>\n" +
				"                <name>ranger.tag.store.file.dir</name>\n" +
				"                <value>file:///etc/ranger/data</value>\n" +
				"        </property>\n" +
				"        <property>\n" +
				"                <name>ranger.service.store.file.dir</name>\n" +
				"                <value>file:///etc/ranger/data</value>\n" +
				"        </property>\n" +
				"</configuration>\n");

		writer.close();

		RangerConfiguration config = RangerConfiguration.getInstance();
		config.addResource(filePath);

		tagStore = TagFileStore.getInstance();
		tagStore.init();

		ServiceStore svcStore;

		svcStore = new ServiceFileStore();
		svcStore.init();

		tagStore.setServiceStore(svcStore);

		validator = new TagValidator();

		validator.setTagStore(tagStore);

		gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z")
				.setPrettyPrinting()
				.create();

		InputStream inStream = TestTagStore.class.getResourceAsStream(serviceDefJsonFile);
		InputStreamReader reader = new InputStreamReader(inStream);
		serviceDef = gsonBuilder.fromJson(reader, RangerServiceDef.class);
		service = svcStore.createService(new RangerService(serviceDef.getName(), serviceName, serviceName, null, null));
		reader.close();
		inStream.close();

	}

	//@AfterClass
	public static void tearDownAfterClass(String suffix) throws Exception {

		Path dirPath = new Path("file:///etc/ranger/data");
		FileSystem fs = dirPath.getFileSystem(config);

		try {
			if (fs.exists(dirPath) && fs.isDirectory(dirPath)) {

				RemoteIterator<LocatedFileStatus> files = fs.listFiles(dirPath, false);

				if (files != null) {
					while (files.hasNext()) {
						LocatedFileStatus fileStatus = files.next();
						Path path = fileStatus.getPath();
						if (fs.isFile(path) && path.getName().endsWith(suffix)) {
							fs.delete(path, true);
						}
					}
				}
			}
		} catch (IOException excp) {
		}

		fs.delete(filePath, true);
	}

	@Test
	public void testTagStore_tag() throws Exception {

		String tagName = "ssn";
		String newTagName = "new-ssn";

		List<RangerTag> tags = tagStore.getTags(filter);

		int initTagCount = tags == null ? 0 : tags.size();

		RangerTag tag = new RangerTag(tagName, new HashMap<String, String>());
		tag.setGuid("GUID_TAG_TEST");

		validator.preCreateTag(tag);
		RangerTag createdTag = tagStore.createTag(tag);

		assertNotNull("createTag() failed", createdTag);
		assertTrue("createTag() name mismatch", createdTag.getName().equals(tag.getName()));
		assertTrue("createTag() GUID mismatch", createdTag.getGuid().equals(tag.getGuid()));

		tags = tagStore.getTags(filter);

		assertEquals("createTag() failed", initTagCount + 1, tags == null ? 0 : tags.size());

		createdTag.setName(newTagName);
		validator.preUpdateTagById(createdTag.getId(), createdTag);
		RangerTag updatedTag = tagStore.updateTag(createdTag);

		tag = tagStore.getTagById(updatedTag.getId());

		assertTrue("updateTag() name mismatch", tag.getName().equals(updatedTag.getName()));
		assertTrue("updatedTag() GUID mismatch", tag.getGuid().equals(updatedTag.getGuid()));

		validator.preDeleteTagById(createdTag.getId());
		tagStore.deleteTagById(createdTag.getId());

		tags = tagStore.getTags(filter);

		assertEquals("deleteTag() failed", initTagCount, tags == null ? 0 : tags.size());

		// Try deleting it again
		try {
			validator.preDeleteTagById(createdTag.getId());
			tagStore.deleteTagById(createdTag.getId());
			assertTrue("deleteTag() failed. Deleted tag again successfully? ", false);
		} catch (Exception exception) {
			assertTrue(true);
		}
	}

	@Test
	public void testTagStore_serviceresource() throws Exception {

		String externalId = "GUID_SERVICERESOURCE_TEST";
		String newExternalId = "NEW_GUID_SERVICERESOURCE_TEST";

		Map<String, RangerPolicyResource> resourceResources = new HashMap<String, RangerPolicyResource>();

		RangerPolicyResource resource = new RangerPolicyResource();
		resource.setValue("*");
		resourceResources.put("database", resource);

		List<RangerServiceResource> serviceResources = tagStore.getServiceResources(filter);

		int initServiceResourceCount = serviceResources == null ? 0 : serviceResources.size();

		RangerServiceResource serviceResource = new RangerServiceResource();
		serviceResource.setServiceName(serviceName);
		serviceResource.setResourceSpec(resourceResources);
		serviceResource.setGuid(externalId);

		validator.preCreateServiceResource(serviceResource);
		RangerServiceResource createdServiceResource = tagStore.createServiceResource(serviceResource);

		assertNotNull("createServiceResource() failed", createdServiceResource);
		assertTrue("createServiceResource() GUID mismatch", createdServiceResource.getGuid().equals(createdServiceResource.getGuid()));

		serviceResources = tagStore.getServiceResources(filter);

		assertEquals("createServiceResource() failed", initServiceResourceCount + 1, serviceResources == null ? 0 : serviceResources.size());

		createdServiceResource.setGuid(newExternalId);
		validator.preUpdateServiceResourceById(createdServiceResource.getId(), createdServiceResource);
		RangerServiceResource updatedServiceResource = tagStore.updateServiceResource(createdServiceResource);

		serviceResource = tagStore.getServiceResourceById(updatedServiceResource.getId());

		assertTrue("updatedServiceResource() GUID mismatch", serviceResource.getGuid().equals(updatedServiceResource.getGuid()));

		validator.preDeleteServiceResourceById(updatedServiceResource.getId());
		tagStore.deleteServiceResourceById(updatedServiceResource.getId());

		serviceResources = tagStore.getServiceResources(filter);

		assertEquals("deleteServiceResource() failed", initServiceResourceCount, serviceResources == null ? 0 : serviceResources.size());

		// Try deleting it again
		try {
			validator.preDeleteServiceResourceById(createdServiceResource.getId());
			tagStore.deleteServiceResourceById(createdServiceResource.getId());
			assertTrue("deleteServiceResource() failed. Deleted serviceResource again successfully? ", false);
		} catch (Exception exception) {
			assertTrue(true);
		}
	}

	@Test
	public void testTagStore_tagResourceMap() throws Exception {

		String tagName = "ssn";

		String externalResourceId = "GUID_SERVICERESOURCE_TEST";
		String externalTagId = "GUID_TAG_TEST";

		List<RangerTag> tags = tagStore.getTags(filter);

		int initTagCount = tags == null ? 0 : tags.size();

		RangerTag tag = new RangerTag(tagName, new HashMap<String, String>());
		tag.setGuid(externalTagId);

		validator.preCreateTag(tag);
		RangerTag createdTag = tagStore.createTag(tag);

		assertNotNull("createTag() failed", createdTag);
		tags = tagStore.getTags(filter);

		assertEquals("createTag() failed", initTagCount + 1, tags == null ? 0 : tags.size());

		Map<String, RangerPolicyResource> resourceResources = new HashMap<String, RangerPolicyResource>();

		RangerPolicyResource resource = new RangerPolicyResource();
		resource.setValue("*");
		resourceResources.put("database", resource);

		List<RangerServiceResource> serviceResources = tagStore.getServiceResources(filter);

		int initServiceResourceCount = serviceResources == null ? 0 : serviceResources.size();

		RangerServiceResource serviceResource = new RangerServiceResource();
		serviceResource.setServiceName(serviceName);
		serviceResource.setResourceSpec(resourceResources);

		serviceResource.setGuid(externalResourceId);
		validator.preCreateServiceResource(serviceResource);
		RangerServiceResource createdServiceResource = tagStore.createServiceResource(serviceResource);

		assertNotNull("createServiceResource() failed", createdServiceResource);

		serviceResources = tagStore.getServiceResources(filter);

		assertEquals("createServiceResource() failed", initServiceResourceCount + 1, serviceResources == null ? 0 : serviceResources.size());

		// Now create map

		RangerTagResourceMap tagResourceMap = validator.preCreateTagResourceMap(externalResourceId, externalTagId);

		RangerTagResourceMap createdTagResourceMap = tagStore.createTagResourceMap(tagResourceMap);

		assertNotNull("createTagResourceMap() failed", createdTagResourceMap);

		ServiceTags serviceTags = tagStore.getServiceTagsIfUpdated(serviceName, -1L);
		List<RangerServiceResource> resourceList = serviceTags.getServiceResources();

		assertTrue("No tagged resources found!", CollectionUtils.isNotEmpty(resourceList) && CollectionUtils.size(resourceList) == 1);

		// Delete all created entities
		RangerTagResourceMap map = validator.preDeleteTagResourceMap(externalResourceId, externalTagId);
		tagStore.deleteTagResourceMapById(map.getId());

		validator.preDeleteServiceResourceById(createdServiceResource.getId());
		tagStore.deleteServiceResourceById(createdServiceResource.getId());

		validator.preDeleteTagById(createdTag.getId());
		tagStore.deleteTagById(createdTag.getId());

	}
}



