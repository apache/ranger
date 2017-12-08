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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.tagsync.source.atlas.AtlasHdfsResourceMapper;
import org.apache.ranger.tagsync.source.atlas.AtlasResourceMapper;
import org.apache.ranger.tagsync.source.atlasrest.RangerAtlasEntity;
import org.junit.Test;

import org.mockito.Mockito;
import org.junit.Assert;



public class TestHdfsResourceMapper {
	private static final String CLUSTER_NAME   = "cl1";
	private static final String PATH           = "hdfs://localhost:8020/user/testuser/finance";
	private static final String QUALIFIED_NAME = "hdfs://localhost:8020/user/testuser/finance@cl1";

	private static final String SERVICE_NAME = "cl1_hadoop";
	private static final String RANGER_PATH  = "/user/testuser/finance";

	AtlasHdfsResourceMapper resourceMapper = new AtlasHdfsResourceMapper();
	AtlasHdfsResourceMapper resourceMapperWithDefaultClusterName = new AtlasHdfsResourceMapper();

	{
		Properties properties = new Properties();

		properties.setProperty(AtlasResourceMapper.TAGSYNC_DEFAULT_CLUSTER_NAME, CLUSTER_NAME);

		resourceMapperWithDefaultClusterName.initialize(properties);
	}

	@Test
	public void testHdfsResourceFromPathAndClusterName() throws Exception {
		Map<String, Object> entAttribs = new HashMap<String, Object>();

		entAttribs.put(AtlasHdfsResourceMapper.ENTITY_ATTRIBUTE_PATH, PATH);
		entAttribs.put(AtlasHdfsResourceMapper.ENTITY_ATTRIBUTE_CLUSTER_NAME, CLUSTER_NAME);

		RangerAtlasEntity entity   = getHdfsPathEntity(entAttribs);
		RangerServiceResource  resource = resourceMapper.buildResource(entity);

		assertServiceResource(resource);
	}

	@Test
	public void testHdfsResourceFromPathAndQualifiedName() throws Exception {
		Map<String, Object> entAttribs = new HashMap<String, Object>();

		entAttribs.put(AtlasHdfsResourceMapper.ENTITY_ATTRIBUTE_PATH, PATH);
		entAttribs.put(AtlasHdfsResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME, QUALIFIED_NAME);

		RangerAtlasEntity entity   = getHdfsPathEntity(entAttribs);
		RangerServiceResource  resource = resourceMapper.buildResource(entity);

		assertServiceResource(resource);
	}

	@Test
	public void testHdfsResourceFromClusterNameAndQualifiedName() throws Exception {
		Map<String, Object> entAttribs = new HashMap<String, Object>();

		entAttribs.put(AtlasHdfsResourceMapper.ENTITY_ATTRIBUTE_CLUSTER_NAME, CLUSTER_NAME);
		entAttribs.put(AtlasHdfsResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME, QUALIFIED_NAME);

		RangerAtlasEntity entity   = getHdfsPathEntity(entAttribs);
		RangerServiceResource  resource = resourceMapper.buildResource(entity);

		assertServiceResource(resource);
	}

	@Test
	public void testHdfsResourceFromPathAndClusterNameAndQualifiedName() throws Exception {
		Map<String, Object> entAttribs = new HashMap<String, Object>();

		entAttribs.put(AtlasHdfsResourceMapper.ENTITY_ATTRIBUTE_PATH, PATH);
		entAttribs.put(AtlasHdfsResourceMapper.ENTITY_ATTRIBUTE_CLUSTER_NAME, CLUSTER_NAME);
		entAttribs.put(AtlasHdfsResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME, QUALIFIED_NAME);

		RangerAtlasEntity entity   = getHdfsPathEntity(entAttribs);
		RangerServiceResource  resource = resourceMapper.buildResource(entity);

		assertServiceResource(resource);
	}

	@Test
	public void testHdfsResourceFromQualifiedNameAndClusterNameFromDefault() throws Exception {
		Map<String, Object> entAttribs = new HashMap<String, Object>();

		entAttribs.put(AtlasHdfsResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME, PATH);

		RangerAtlasEntity entity   = getHdfsPathEntity(entAttribs);
		RangerServiceResource  resource = resourceMapperWithDefaultClusterName.buildResource(entity);

		assertServiceResource(resource);
	}

	@Test
	public void testHdfsResourceFromPathAndClusterNameFromDefault() throws Exception {
		Map<String, Object> entAttribs = new HashMap<String, Object>();

		entAttribs.put(AtlasHdfsResourceMapper.ENTITY_ATTRIBUTE_PATH, PATH);

		RangerAtlasEntity entity   = getHdfsPathEntity(entAttribs);
		RangerServiceResource  resource = resourceMapperWithDefaultClusterName.buildResource(entity);

		assertServiceResource(resource);
	}

	@Test
	public void testHdfsResourceFromMissingAttribs() throws Exception {
		Map<String, Object> entAttribs = new HashMap<String, Object>();

		RangerAtlasEntity entity  = getHdfsPathEntity(entAttribs);

		try {
			RangerServiceResource resource = resourceMapper.buildResource(entity);

			Assert.fail("expected exception. Found " + resource);
		} catch(Exception excp) {
			// ignore
		}
	}

	private RangerAtlasEntity getHdfsPathEntity(Map<String, Object> entAttribs) throws Exception {
		RangerAtlasEntity entity = Mockito.mock(RangerAtlasEntity.class);

		Mockito.when(entity.getTypeName()).thenReturn(AtlasHdfsResourceMapper.ENTITY_TYPE_HDFS_PATH);
        Mockito.when(entity.getAttributes()).thenReturn(entAttribs);
        Mockito.when(entity.getAttributes()).thenReturn(entAttribs);
        Mockito.when(entity.getAttributes()).thenReturn(entAttribs);

		return entity;
	}

	private void assertServiceResource(RangerServiceResource resource) {
		Assert.assertNotNull(resource);
		Assert.assertEquals(SERVICE_NAME, resource.getServiceName());
		Assert.assertNotNull(resource.getResourceElements());
		Assert.assertEquals(1, resource.getResourceElements().size());
		Assert.assertTrue(resource.getResourceElements().containsKey(AtlasHdfsResourceMapper.RANGER_TYPE_HDFS_PATH));
		Assert.assertNotNull(resource.getResourceElements().get(AtlasHdfsResourceMapper.RANGER_TYPE_HDFS_PATH).getValues());
		Assert.assertEquals(1, resource.getResourceElements().get(AtlasHdfsResourceMapper.RANGER_TYPE_HDFS_PATH).getValues().size());
		Assert.assertEquals(RANGER_PATH, resource.getResourceElements().get(AtlasHdfsResourceMapper.RANGER_TYPE_HDFS_PATH).getValues().get(0));
	}
}
