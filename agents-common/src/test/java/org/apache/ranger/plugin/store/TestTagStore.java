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

import java.io.*;
import java.util.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.*;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.store.file.ServiceFileStore;
import org.apache.ranger.plugin.store.file.TagFileStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceTags;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;


public class TestTagStore {
	static TagStore tagStore = null;
	static TagValidator validator = null;
	static RangerServiceDef serviceDef = null;
	static RangerService service = null;
	static SearchFilter filter = null;

	static final String serviceDefJsonFile = "/admin/service-defs/test-hive-servicedef.json";
	static final String serviceName = "tag-unit-test-TestTagStore";
	static File tagStoreDir = null;

	static Gson gsonBuilder = null;

	@BeforeClass
	public static void setupTest() throws Exception {

		String textTemplate = "<configuration>\n" +
				"        <property>\n" +
				"                <name>ranger.tag.store.file.dir</name>\n" +
				"                <value>%s</value>\n" +
				"        </property>\n" +
				"        <property>\n" +
				"                <name>ranger.service.store.file.dir</name>\n" +
				"                <value>%s</value>\n" +
				"        </property>\n" +
				"</configuration>\n";

		File file = File.createTempFile("ranger-admin-test-site", ".xml") ;
		file.deleteOnExit();

		tagStoreDir = File.createTempFile("tagStore", "dir") ;

		if (tagStoreDir.exists()) {
			tagStoreDir.delete() ;
		}

		tagStoreDir.mkdirs() ;

		String tagStoreDirName =  tagStoreDir.getAbsolutePath() ;

		String text = String.format(textTemplate, tagStoreDirName, tagStoreDirName);

		FileOutputStream outStream = new FileOutputStream(file);
		OutputStreamWriter writer = new OutputStreamWriter(outStream);
		writer.write(text);
		writer.close();

		RangerConfiguration config = RangerConfiguration.getInstance();
		config.addResource(new org.apache.hadoop.fs.Path(file.toURI()));

		ServiceStore svcStore = new ServiceFileStore();
		svcStore.init();

		tagStore = TagFileStore.getInstance();
		tagStore.init();
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

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		if (tagStoreDir != null) {
			try {
				File[] filesInTagStoreDir = tagStoreDir.listFiles();
				if (filesInTagStoreDir != null) {
					for (File file : filesInTagStoreDir) {
						if (file.isFile()) {
							file.delete();
						}
					}
				}
				tagStoreDir.delete();
				tagStoreDir = null;
			} catch (Throwable t) {
				// Ignore
			}
		}
	}

	@Test
	public void testTagStore_tag() throws Exception {

		String tagType = "ssn";
		String newTagType = "new-ssn";

		List<RangerTag> tags = tagStore.getTags(filter);

		int initTagCount = tags == null ? 0 : tags.size();

		RangerTag tag = new RangerTag(tagType, new HashMap<String, String>());
		tag.setGuid("GUID_TAG_TEST");

		validator.preCreateTag(tag);
		RangerTag createdTag = tagStore.createTag(tag);

		assertNotNull("createTag() failed", createdTag);
		assertTrue("createTag() type mismatch", createdTag.getType().equals(tag.getType()));
		assertTrue("createTag() GUID mismatch", createdTag.getGuid().equals(tag.getGuid()));

		tags = tagStore.getTags(filter);

		assertEquals("createTag() failed", initTagCount + 1, tags == null ? 0 : tags.size());

		createdTag.setType(newTagType);
		validator.preUpdateTag(createdTag.getId(), createdTag);
		RangerTag updatedTag = tagStore.updateTag(createdTag);

		tag = tagStore.getTag(updatedTag.getId());

		assertTrue("updateTag() type mismatch", tag.getType().equals(updatedTag.getType()));
		assertTrue("updatedTag() GUID mismatch", tag.getGuid().equals(updatedTag.getGuid()));

		validator.preDeleteTag(createdTag.getId());
		tagStore.deleteTag(createdTag.getId());

		tags = tagStore.getTags(filter);

		assertEquals("deleteTag() failed", initTagCount, tags == null ? 0 : tags.size());

		// Try deleting it again
		try {
			validator.preDeleteTag(createdTag.getId());
			tagStore.deleteTag(createdTag.getId());
			assertTrue("deleteTag() failed. Deleted tag again successfully? ", false);
		} catch (Exception exception) {
			assertTrue(true);
		}
	}

	@Test
	public void testTagStore_serviceresource() throws Exception {

		String guid = "GUID_SERVICERESOURCE_TEST";
		String newGuid = "NEW_GUID_SERVICERESOURCE_TEST";

		Map<String, RangerPolicyResource> resourceElements = new HashMap<String, RangerPolicyResource>();

		RangerPolicyResource resourceElement = new RangerPolicyResource();
		resourceElement.setValue("*");
		resourceElements.put("database", resourceElement);

		List<RangerServiceResource> serviceResources = tagStore.getServiceResources(filter);

		int initServiceResourceCount = serviceResources == null ? 0 : serviceResources.size();

		RangerServiceResource serviceResource = new RangerServiceResource();
		serviceResource.setServiceName(serviceName);
		serviceResource.setResourceElements(resourceElements);
		serviceResource.setGuid(guid);

		validator.preCreateServiceResource(serviceResource);
		RangerServiceResource createdServiceResource = tagStore.createServiceResource(serviceResource);

		assertNotNull("createServiceResource() failed", createdServiceResource);
		assertTrue("createServiceResource() GUID mismatch", createdServiceResource.getGuid().equals(createdServiceResource.getGuid()));

		serviceResources = tagStore.getServiceResources(filter);

		assertEquals("createServiceResource() failed", initServiceResourceCount + 1, serviceResources == null ? 0 : serviceResources.size());

		createdServiceResource.setGuid(newGuid);
		validator.preUpdateServiceResource(createdServiceResource.getId(), createdServiceResource);
		RangerServiceResource updatedServiceResource = tagStore.updateServiceResource(createdServiceResource);

		serviceResource = tagStore.getServiceResource(updatedServiceResource.getId());

		assertTrue("updatedServiceResource() GUID mismatch", serviceResource.getGuid().equals(updatedServiceResource.getGuid()));

		validator.preDeleteServiceResource(updatedServiceResource.getId());
		tagStore.deleteServiceResource(updatedServiceResource.getId());

		serviceResources = tagStore.getServiceResources(filter);

		assertEquals("deleteServiceResource() failed", initServiceResourceCount, serviceResources == null ? 0 : serviceResources.size());

		// Try deleting it again
		try {
			validator.preDeleteServiceResource(createdServiceResource.getId());
			tagStore.deleteServiceResource(createdServiceResource.getId());
			assertTrue("deleteServiceResource() failed. Deleted serviceResource again successfully? ", false);
		} catch (Exception exception) {
			assertTrue(true);
		}
	}

	@Test
	public void testTagStore_tagResourceMap() throws Exception {

		String tagType = "ssn";

		String resourceGuid = "GUID_SERVICERESOURCE_TEST";
		String tagGuid = "GUID_TAG_TEST";

		List<RangerTag> tags = tagStore.getTags(filter);

		int initTagCount = tags == null ? 0 : tags.size();

		RangerTag tag = new RangerTag(tagType, new HashMap<String, String>());
		tag.setGuid(tagGuid);

		validator.preCreateTag(tag);
		RangerTag createdTag = tagStore.createTag(tag);

		assertNotNull("createTag() failed", createdTag);
		tags = tagStore.getTags(filter);

		assertEquals("createTag() failed", initTagCount + 1, tags == null ? 0 : tags.size());

		Map<String, RangerPolicyResource> resourceElements = new HashMap<String, RangerPolicyResource>();

		RangerPolicyResource resource = new RangerPolicyResource();
		resource.setValue("*");
		resourceElements.put("database", resource);

		List<RangerServiceResource> serviceResources = tagStore.getServiceResources(filter);

		int initServiceResourceCount = serviceResources == null ? 0 : serviceResources.size();

		RangerServiceResource serviceResource = new RangerServiceResource();
		serviceResource.setServiceName(serviceName);
		serviceResource.setResourceElements(resourceElements);

		serviceResource.setGuid(resourceGuid);
		validator.preCreateServiceResource(serviceResource);
		RangerServiceResource createdServiceResource = tagStore.createServiceResource(serviceResource);

		assertNotNull("createServiceResource() failed", createdServiceResource);

		serviceResources = tagStore.getServiceResources(filter);

		assertEquals("createServiceResource() failed", initServiceResourceCount + 1, serviceResources == null ? 0 : serviceResources.size());

		// Now create map

		RangerTagResourceMap tagResourceMap = validator.preCreateTagResourceMap(tagGuid, resourceGuid);

		RangerTagResourceMap createdTagResourceMap = tagStore.createTagResourceMap(tagResourceMap);

		assertNotNull("createTagResourceMap() failed", createdTagResourceMap);

		ServiceTags serviceTags = tagStore.getServiceTagsIfUpdated(serviceName, -1L);
		List<RangerServiceResource> resourceList = serviceTags.getServiceResources();

		assertTrue("No tagged resources found!", CollectionUtils.isNotEmpty(resourceList) && CollectionUtils.size(resourceList) == 1);

		// Delete all created entities
		RangerTagResourceMap map = validator.preDeleteTagResourceMap(tagGuid, resourceGuid);
		tagStore.deleteTagResourceMap(map.getId());

		validator.preDeleteServiceResource(createdServiceResource.getId());
		tagStore.deleteServiceResource(createdServiceResource.getId());

		// private tags are deleted when TagResourceMap is deleted.. No need for deleting it here
		//validator.preDeleteTag(createdTag.getId());
		//tagStore.deleteTag(createdTag.getId());

	}
}



