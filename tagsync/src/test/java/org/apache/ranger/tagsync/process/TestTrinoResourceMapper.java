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
import org.apache.ranger.tagsync.source.atlas.AtlasTrinoResourceMapper;
import org.apache.ranger.tagsync.source.atlasrest.RangerAtlasEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.ranger.tagsync.source.atlas.AtlasResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME;
import static org.apache.ranger.tagsync.source.atlas.AtlasTrinoResourceMapper.ENTITY_TYPE_TRINO_CATALOG;
import static org.apache.ranger.tagsync.source.atlas.AtlasTrinoResourceMapper.ENTITY_TYPE_TRINO_COLUMN;
import static org.apache.ranger.tagsync.source.atlas.AtlasTrinoResourceMapper.ENTITY_TYPE_TRINO_INSTANCE;
import static org.apache.ranger.tagsync.source.atlas.AtlasTrinoResourceMapper.ENTITY_TYPE_TRINO_SCHEMA;
import static org.apache.ranger.tagsync.source.atlas.AtlasTrinoResourceMapper.ENTITY_TYPE_TRINO_TABLE;
import static org.apache.ranger.tagsync.source.atlas.AtlasTrinoResourceMapper.RANGER_TYPE_TRINO_CATALOG;
import static org.apache.ranger.tagsync.source.atlas.AtlasTrinoResourceMapper.RANGER_TYPE_TRINO_COLUMN;
import static org.apache.ranger.tagsync.source.atlas.AtlasTrinoResourceMapper.RANGER_TYPE_TRINO_SCHEMA;
import static org.apache.ranger.tagsync.source.atlas.AtlasTrinoResourceMapper.RANGER_TYPE_TRINO_TABLE;

public class TestTrinoResourceMapper {
    private static final String INSTANCE_QUALIFIED_NAME         = "dev";
    private static final String CATALOG_QUALIFIED_NAME          = "sales@dev";
    private static final String SCHEMA_QUALIFIED_NAME           = "sales.reporting@dev";
    private static final String TABLE_QUALIFIED_NAME            = "sales.reporting.orders@dev";
    private static final String COLUMN_QUALIFIED_NAME           = "sales.reporting.orders.customer_id@dev";
    private static final String INVALID_RESOURCE_QUALIFIED_NAME = "sales.reporting.orders.customer_id.extra@dev";

    private static final String SERVICE_NAME    = "dev_trino";
    private static final String RANGER_CATALOG  = "sales";
    private static final String RANGER_SCHEMA   = "reporting";
    private static final String RANGER_TABLE    = "orders";
    private static final String RANGER_COLUMN   = "customer_id";

    AtlasTrinoResourceMapper resourceMapper = new AtlasTrinoResourceMapper();

    @Test
    public void testTrinoCatalog() throws Exception {
        RangerAtlasEntity     entity   = getEntity(ENTITY_TYPE_TRINO_CATALOG, CATALOG_QUALIFIED_NAME);
        RangerServiceResource resource = resourceMapper.buildResource(entity);

        Assertions.assertEquals(SERVICE_NAME, resource.getServiceName());
        assertCatalogResource(resource);
    }

    @Test
    public void testTrinoSchema() throws Exception {
        RangerAtlasEntity     entity   = getEntity(ENTITY_TYPE_TRINO_SCHEMA, SCHEMA_QUALIFIED_NAME);
        RangerServiceResource resource = resourceMapper.buildResource(entity);

        Assertions.assertEquals(SERVICE_NAME, resource.getServiceName());
        assertSchemaResource(resource);
    }

    @Test
    public void testTrinoTable() throws Exception {
        RangerAtlasEntity     entity   = getEntity(ENTITY_TYPE_TRINO_TABLE, TABLE_QUALIFIED_NAME);
        RangerServiceResource resource = resourceMapper.buildResource(entity);

        Assertions.assertEquals(SERVICE_NAME, resource.getServiceName());
        assertTableResource(resource);
    }

    @Test
    public void testTrinoColumn() throws Exception {
        RangerAtlasEntity     entity   = getEntity(ENTITY_TYPE_TRINO_COLUMN, COLUMN_QUALIFIED_NAME);
        RangerServiceResource resource = resourceMapper.buildResource(entity);

        Assertions.assertEquals(SERVICE_NAME, resource.getServiceName());
        assertColumnResource(resource);
    }

    @Test
    public void testInvalidCatalogEntity() {
        assertException(getEntity(ENTITY_TYPE_TRINO_CATALOG, null), "attribute 'qualifiedName' not found");
        assertException(getEntity(ENTITY_TYPE_TRINO_CATALOG, ""), "attribute 'qualifiedName' not found");
        assertException(getEntity(ENTITY_TYPE_TRINO_CATALOG, "sales"), "trino-instance not found");
    }

    @Test
    public void testInvalidSchemaEntity() {
        assertException(getEntity(ENTITY_TYPE_TRINO_SCHEMA, null), "attribute 'qualifiedName' not found");
        assertException(getEntity(ENTITY_TYPE_TRINO_SCHEMA, ""), "attribute 'qualifiedName' not found");
        assertException(getEntity(ENTITY_TYPE_TRINO_SCHEMA, "sales.reporting"), "trino-instance not found");
        assertException(getEntity(ENTITY_TYPE_TRINO_SCHEMA, CATALOG_QUALIFIED_NAME), "invalid qualifiedName");
    }

    @Test
    public void testInvalidTableEntity() {
        assertException(getEntity(ENTITY_TYPE_TRINO_TABLE, null), "attribute 'qualifiedName' not found");
        assertException(getEntity(ENTITY_TYPE_TRINO_TABLE, ""), "attribute 'qualifiedName' not found");
        assertException(getEntity(ENTITY_TYPE_TRINO_TABLE, "sales.reporting.orders"), "trino-instance not found");
        assertException(getEntity(ENTITY_TYPE_TRINO_TABLE, SCHEMA_QUALIFIED_NAME), "invalid qualifiedName");
    }

    @Test
    public void testInvalidColumnEntity() {
        assertException(getEntity(ENTITY_TYPE_TRINO_COLUMN, null), "attribute 'qualifiedName' not found");
        assertException(getEntity(ENTITY_TYPE_TRINO_COLUMN, ""), "attribute 'qualifiedName' not found");
        assertException(getEntity(ENTITY_TYPE_TRINO_COLUMN, "sales.reporting.orders.customer_id"), "trino-instance not found");
        assertException(getEntity(ENTITY_TYPE_TRINO_COLUMN, TABLE_QUALIFIED_NAME), "invalid qualifiedName");
        assertException(getEntity(ENTITY_TYPE_TRINO_COLUMN, INVALID_RESOURCE_QUALIFIED_NAME), "invalid resource format");
    }

    @Test
    public void testInvalidInstanceEntity() {
        assertException(getEntity(ENTITY_TYPE_TRINO_INSTANCE, null), "attribute 'qualifiedName' not found");
        assertException(getEntity(ENTITY_TYPE_TRINO_INSTANCE, ""), "attribute 'qualifiedName' not found");
        assertException(getEntity(ENTITY_TYPE_TRINO_INSTANCE, INSTANCE_QUALIFIED_NAME), "trino-instance not found");
    }

    private RangerAtlasEntity getEntity(String entityType, String qualifiedName) {
        return new RangerAtlasEntity(entityType, "guid-" + entityType, Collections.singletonMap(ENTITY_ATTRIBUTE_QUALIFIED_NAME, qualifiedName));
    }

    private void assertResourceElementCount(RangerServiceResource resource, int count) {
        Assertions.assertNotNull(resource);
        Assertions.assertEquals(SERVICE_NAME, resource.getServiceName());
        Assertions.assertNotNull(resource.getResourceElements());
        Assertions.assertEquals(count, resource.getResourceElements().size());
    }

    private void assertCatalogResource(RangerServiceResource resource) {
        assertResourceElementCount(resource, 1);
        assertResourceElementValue(resource, RANGER_TYPE_TRINO_CATALOG, RANGER_CATALOG);
    }

    private void assertSchemaResource(RangerServiceResource resource) {
        assertResourceElementCount(resource, 2);
        assertResourceElementValue(resource, RANGER_TYPE_TRINO_CATALOG, RANGER_CATALOG);
        assertResourceElementValue(resource, RANGER_TYPE_TRINO_SCHEMA, RANGER_SCHEMA);
    }

    private void assertTableResource(RangerServiceResource resource) {
        assertResourceElementCount(resource, 3);
        assertResourceElementValue(resource, RANGER_TYPE_TRINO_CATALOG, RANGER_CATALOG);
        assertResourceElementValue(resource, RANGER_TYPE_TRINO_SCHEMA, RANGER_SCHEMA);
        assertResourceElementValue(resource, RANGER_TYPE_TRINO_TABLE, RANGER_TABLE);
    }

    private void assertColumnResource(RangerServiceResource resource) {
        assertResourceElementCount(resource, 4);
        assertResourceElementValue(resource, RANGER_TYPE_TRINO_CATALOG, RANGER_CATALOG);
        assertResourceElementValue(resource, RANGER_TYPE_TRINO_SCHEMA, RANGER_SCHEMA);
        assertResourceElementValue(resource, RANGER_TYPE_TRINO_TABLE, RANGER_TABLE);
        assertResourceElementValue(resource, RANGER_TYPE_TRINO_COLUMN, RANGER_COLUMN);
    }

    private void assertResourceElementValue(RangerServiceResource resource, String resourceName, String value) {
        Assertions.assertTrue(resource.getResourceElements().containsKey(resourceName));
        Assertions.assertNotNull(resource.getResourceElements().get(resourceName).getValues());
        Assertions.assertEquals(1, resource.getResourceElements().get(resourceName).getValues().size());
        Assertions.assertEquals(value, resource.getResourceElements().get(resourceName).getValues().get(0));
    }

    private void assertException(RangerAtlasEntity entity, String exceptionMessage) {
        try {
            RangerServiceResource resource = resourceMapper.buildResource(entity);

            Assertions.assertFalse(true, "Expected buildResource() to fail. But it returned " + resource);
        } catch (Exception excp) {
            Assertions.assertTrue(excp.getMessage().startsWith(exceptionMessage), "Unexpected exception message: expected=" + exceptionMessage + "; found " + excp.getMessage());
        }
    }
}
