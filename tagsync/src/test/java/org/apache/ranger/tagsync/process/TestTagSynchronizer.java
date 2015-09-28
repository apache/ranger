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

package org.apache.ranger.tagsync.process;


import org.apache.ranger.tagsync.model.TagSource;
import org.apache.ranger.tagsync.process.TagSyncConfig;
import org.apache.ranger.tagsync.process.TagSynchronizer;
import org.apache.ranger.tagsync.source.atlas.TagAtlasSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.util.Properties;

import static org.junit.Assert.*;


public class TestTagSynchronizer {

	private static TagSynchronizer tagSynchronizer;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		System.out.println("setUpBeforeClass() called");

		TagSyncConfig config = TagSyncConfig.getInstance();

		TagSyncConfig.dumpConfiguration(config, new BufferedWriter(new OutputStreamWriter(System.out)));

		Properties props = config.getProperties();

		tagSynchronizer = new TagSynchronizer(props);

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		System.out.println("tearDownAfterClass() called");

	}

	@Test
	public void testTagSynchronizer() {

		System.out.println("testTagSynchronizer() called");

		//tagSynchronizer.run();

		tagSynchronizer.shutdown("From testTagSynchronizer: time=up");

		System.out.println("Exiting test");


	}

	@Test
	public void testTagDownload() {

		boolean initDone = tagSynchronizer.initLoop();

		System.out.println("TagSynchronizer initialization result=" + initDone);

		/*
		TagSource tagSource = tagSynchronizer.getTagSource();

		try {
			TagAtlasSource tagAtlasSource = (TagAtlasSource) tagSource;
			//tagAtlasSource.printAllEntities();
		} catch (ClassCastException exception) {
			System.err.println("TagSource is not of TagAtlasSource");
		}
		*/

		System.out.println("Exiting testTagDownload()");
	}
}
