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
import java.io.OutputStreamWriter;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.*;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.store.file.TagFileStore;
import org.apache.ranger.plugin.store.rest.ServiceRESTStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestTagStore {
	static TagStore tagStore = null;
	static SearchFilter filter   = null;
	static Path filePath = new Path("file:///etc/ranger/data/ranger-admin-test-site.xml");
	static Configuration config = new Configuration();

	static final String sdName      = "tagDef-unit-test-TestTagStore";
	static final String serviceName = "tag-unit-test-TestTagStore";
	static final String policyName  = "tag-1";

	@BeforeClass
	public static void setupTest() throws Exception {

		/*
		tearDownAfterClass();

		FileSystem fs = filePath.getFileSystem(config);

		FSDataOutputStream outStream  = fs.create(filePath, true);
		OutputStreamWriter writer = null;


		writer = new OutputStreamWriter(outStream);

		writer.write("<configuration>\n" +
				"        <property>\n" +
				"                <name>ranger.service.store.rest.url</name>\n" +
				"                <value>http://node-1.example.com:6080</value>\n" +
				"        </property>\n" +
				"        <property>\n" +
				"                <name>ranger.service.store.rest.basicauth.username</name>\n" +
				"                <value>admin</value>\n" +
				"        </property>\n" +
				"        <property>\n" +
				"                <name>ranger.service.store.rest.basicauth.password</name>\n" +
				"                <value>admin</value>\n" +
				"        </property>\n" +
				"        <property>\n" +
				"                <name>ranger.tag.store.file.dir</name>\n" +
				"                <value>file:///etc/ranger/data</value>\n" +
				"        </property>\n" +
				"</configuration>\n");

		writer.close();

		RangerConfiguration config = RangerConfiguration.getInstance();
		config.addResource(filePath);

		tagStore = TagFileStore.getInstance();
		tagStore.setServiceStore(new ServiceRESTStore());
		tagStore.init();
		*/

	}

	//@AfterClass
	public static void tearDownAfterClass() throws Exception {

		/*
		Path dirPath = new Path("file:///etc/ranger/data");
		FileSystem fs = dirPath.getFileSystem(config);

		try {
			if(fs.exists(dirPath) && fs.isDirectory(dirPath)) {
				PathFilter filter = new PathFilter() {
					@Override
					public boolean accept(Path path) {
						return path.getName().endsWith(".json") ||
								path.getName().endsWith(".crc");
					}
				};

				RemoteIterator<LocatedFileStatus> files = fs.listFiles(dirPath, false);

				if(files != null) {
					while (files.hasNext()) {
						LocatedFileStatus fileStatus = files.next();
						Path path = fileStatus.getPath();
						if (fs.isFile(path) && path.getName().endsWith(".json") || path.getName().endsWith(".crc")) {
							fs.delete(path, true);
						}
					}
				}
			}
		} catch(IOException excp) {
		}

		fs.delete(filePath, true);
		*/
	}

	@Test
	public void testTagStore() throws Exception {

		/*
		List<RangerTaggedResource> taggedResources = tagStore.getResources(filter);

		int initResourceCount = taggedResources == null ? 0 : taggedResources.size();

		RangerTaggedResource rr = new RangerTaggedResource();
		rr.setComponentType("hive");
		rr.setTagServiceName("tagdev");

		Map<String, RangerPolicyResource> resourceSpec = new HashMap<>();

		RangerPolicyResource policyResource = new RangerPolicyResource();
		policyResource.setValues(Arrays.asList("default", "hr", "finance"));
		resourceSpec.put("database", policyResource);

		policyResource = new RangerPolicyResource();
		policyResource.setValues(Arrays.asList("table1", "employee", "invoice"));
		resourceSpec.put("table", policyResource);

		policyResource = new RangerPolicyResource();
		policyResource.setValues(Arrays.asList("column1", "ssn", "vendor"));
		resourceSpec.put("column", policyResource);

		rr.setResourceSpec(resourceSpec);

		List<RangerTaggedResource.RangerResourceTag> tags = new ArrayList<>();

		tags.add(new RangerTaggedResource.RangerResourceTag("PII", null));
		tags.add(new RangerTaggedResource.RangerResourceTag("FINANCE", null));

		rr.setTags(tags);

		RangerTaggedResource createdResource = tagStore.createResource(rr);

		assertNotNull("createResource() failed", createdResource);

		taggedResources = tagStore.getResources(filter);
		assertEquals("createResource() failed", initResourceCount + 1, taggedResources == null ? 0 : taggedResources.size());

		taggedResources = tagStore.getResources("hive", resourceSpec);
		assertEquals("createResource() failed", initResourceCount + 1, taggedResources == null ? 0 : taggedResources.size());

		resourceSpec.remove("column");
		taggedResources = tagStore.getResources("hive", resourceSpec);
		assertEquals("createResource() failed", initResourceCount, taggedResources == null ? 0 : taggedResources.size());
		*/
	}
}
