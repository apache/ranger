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

import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.tagsync.source.atlas.AtlasHiveResourceMapper;
import org.apache.ranger.tagsync.source.atlasrest.RangerAtlasEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class TestHiveResourceMapper {
    private static final String DB_QUALIFIED_NAME     = "default@cl1";
    private static final String TABLE_QUALIFIED_NAME  = "default.testTable@cl1";
    private static final String COLUMN_QUALIFIED_NAME = "default.testTable.col1@cl1";

    private static final String SERVICE_NAME    = "cl1_hive";
    private static final String RANGER_DATABASE = "default";
    private static final String RANGER_TABLE    = "testTable";
    private static final String RANGER_COLUMN   = "col1";

    AtlasHiveResourceMapper resourceMapper = new AtlasHiveResourceMapper();

    @Test
    public void testHiveDb() throws Exception {
        Map<String, Object> entAttribs = new HashMap<String, Object>();

        entAttribs.put(AtlasHiveResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME, DB_QUALIFIED_NAME);

        RangerAtlasEntity     entity   = getHiveDbEntity(entAttribs);
        RangerServiceResource resource = resourceMapper.buildResource(entity);

        assertDbResource(resource);
    }

    @Test
    public void testHiveTable() throws Exception {
        Map<String, Object> entAttribs = new HashMap<String, Object>();

        entAttribs.put(AtlasHiveResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME, TABLE_QUALIFIED_NAME);

        RangerAtlasEntity     entity   = getHiveTableEntity(entAttribs);
        RangerServiceResource resource = resourceMapper.buildResource(entity);

        assertTableResource(resource);
    }

    @Test
    public void testHiveColumn() throws Exception {
        Map<String, Object> entAttribs = new HashMap<String, Object>();

        entAttribs.put(AtlasHiveResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME, COLUMN_QUALIFIED_NAME);

        RangerAtlasEntity     entity   = getHiveColumnEntity(entAttribs);
        RangerServiceResource resource = resourceMapper.buildResource(entity);

        assertColumnResource(resource);
    }

    @Test
    public void testHiveResourceFromMissingAttribs() throws Exception {
        Map<String, Object> entAttribs = new HashMap<String, Object>();

        RangerAtlasEntity entity = getHiveDbEntity(entAttribs);

        try {
            RangerServiceResource resource = resourceMapper.buildResource(entity);

            Assertions.fail("expected exception. Found " + resource);
        } catch (Exception excp) {
            // ignore
        }
    }

    private RangerAtlasEntity getHiveDbEntity(Map<String, Object> entAttribs) throws Exception {
        RangerAtlasEntity entity = Mockito.mock(RangerAtlasEntity.class);

        Mockito.when(entity.getTypeName()).thenReturn(AtlasHiveResourceMapper.ENTITY_TYPE_HIVE_DB);
        Mockito.when(entity.getAttributes()).thenReturn(entAttribs);

        return entity;
    }

    private RangerAtlasEntity getHiveTableEntity(Map<String, Object> entAttribs) throws Exception {
        RangerAtlasEntity entity = Mockito.mock(RangerAtlasEntity.class);

        Mockito.when(entity.getTypeName()).thenReturn(AtlasHiveResourceMapper.ENTITY_TYPE_HIVE_TABLE);
        Mockito.when(entity.getAttributes()).thenReturn(entAttribs);

        return entity;
    }

    private RangerAtlasEntity getHiveColumnEntity(Map<String, Object> entAttribs) throws Exception {
        RangerAtlasEntity entity = Mockito.mock(RangerAtlasEntity.class);

        Mockito.when(entity.getTypeName()).thenReturn(AtlasHiveResourceMapper.ENTITY_TYPE_HIVE_COLUMN);
        Mockito.when(entity.getAttributes()).thenReturn(entAttribs);

        return entity;
    }

    private void assertServiceResource(RangerServiceResource resource) {
        Assertions.assertNotNull(resource);
        Assertions.assertEquals(SERVICE_NAME, resource.getServiceName());
        Assertions.assertNotNull(resource.getResourceElements());
    }

    private void assertDbResource(RangerServiceResource resource) {
        assertServiceResource(resource);

        Assertions.assertEquals(1, resource.getResourceElements().size());
        Assertions.assertTrue(resource.getResourceElements().containsKey(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_DB));
        Assertions.assertNotNull(resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_DB).getValues());
        Assertions.assertEquals(1, resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_DB).getValues().size());
        Assertions.assertEquals(RANGER_DATABASE, resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_DB).getValues().get(0));
    }

    private void assertTableResource(RangerServiceResource resource) {
        assertServiceResource(resource);

        Assertions.assertEquals(2, resource.getResourceElements().size());
        Assertions.assertTrue(resource.getResourceElements().containsKey(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_DB));
        Assertions.assertNotNull(resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_DB).getValues());
        Assertions.assertEquals(1, resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_DB).getValues().size());
        Assertions.assertEquals(RANGER_DATABASE, resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_DB).getValues().get(0));

        Assertions.assertTrue(resource.getResourceElements().containsKey(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_TABLE));
        Assertions.assertNotNull(resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_TABLE).getValues());
        Assertions.assertEquals(1, resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_TABLE).getValues().size());
        Assertions.assertEquals(RANGER_TABLE, resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_TABLE).getValues().get(0));
    }

    private void assertColumnResource(RangerServiceResource resource) {
        assertServiceResource(resource);

        Assertions.assertEquals(3, resource.getResourceElements().size());
        Assertions.assertTrue(resource.getResourceElements().containsKey(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_DB));
        Assertions.assertNotNull(resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_DB).getValues());
        Assertions.assertEquals(1, resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_DB).getValues().size());
        Assertions.assertEquals(RANGER_DATABASE, resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_DB).getValues().get(0));

        Assertions.assertTrue(resource.getResourceElements().containsKey(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_TABLE));
        Assertions.assertNotNull(resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_TABLE).getValues());
        Assertions.assertEquals(1, resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_TABLE).getValues().size());
        Assertions.assertEquals(RANGER_TABLE, resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_TABLE).getValues().get(0));

        Assertions.assertTrue(resource.getResourceElements().containsKey(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_COLUMN));
        Assertions.assertNotNull(resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_COLUMN).getValues());
        Assertions.assertEquals(1, resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_COLUMN).getValues().size());
        Assertions.assertEquals(RANGER_COLUMN, resource.getResourceElements().get(AtlasHiveResourceMapper.RANGER_TYPE_HIVE_COLUMN).getValues().get(0));
    }
}
