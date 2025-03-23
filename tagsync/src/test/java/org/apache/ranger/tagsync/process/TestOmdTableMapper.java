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
import org.apache.ranger.tagsync.source.openmetadatarest.OpenmetadataTableMapper;
import org.apache.ranger.tagsync.source.openmetadatarest.RangerOpenmetadataEntity;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.openmetadata.client.model.Column;
import org.openmetadata.client.model.Table;

import org.openmetadata.client.model.EntityReference;

import java.util.*;

public class TestOmdTableMapper {

    private static final String SCHEMA_QUALIFIED_NAME     = "default_ingestion_service.test_catalog.test_schema";
    private static final String SCHEMA_NAME               = "test_schema";
    private static final String TABLE_QUALIFIED_NAME  = "default_ingestion_service.test_catalog.test_schema.test_table";
    private static final String TABLE_NAME  = "test_table";
    private static final String COLUMN_QUALIFIED_NAME = "default_ingestion_service.test_catalog.test_schema.test_table.test_col";
    private static final String COLUMN_NAME = "test_col";
    private static final String CATALOG_QUALIFIED_NAME     = "default_ingestion_service.test_catalog";
    private static final String CATALOG_NAME     = "test_catalog";
    private static final String SERVICE_QUALIFIED_NAME     = "default_ingestion_service";

    public static final String SERVICE_NAME = "default_ingestion_service_trino";
    public static final String RANGER_TRINO_CATALOG = "test_catalog";
    public static final String RANGER_TRINO_SCHEMA  = "test_schema";
    public static final String RANGER_TRINO_TABLE   = "test_table";
    public static final String RANGER_TRINO_COLUMN  = "test_col";

    OpenmetadataTableMapper resourceMapper = new OpenmetadataTableMapper();

    @Test
    public void testTableEntity() throws Exception {
        Table tableEntity = new Table();
        // Set Fully Qualified name of database AKA catalog in terms of Trino
        EntityReference database = new EntityReference();
        database.setName(CATALOG_NAME);
        database.setFullyQualifiedName(CATALOG_QUALIFIED_NAME);
        tableEntity.setDatabase(database);

        // Set Fully Qualified Name of Service (Service in Openmetadata terms not that of Ranger resource service)
        EntityReference omdTrinoService = new EntityReference();
        omdTrinoService.setFullyQualifiedName(SERVICE_QUALIFIED_NAME);
        tableEntity.setService(omdTrinoService);

        // Set Tablename
        tableEntity.setName(TABLE_NAME);
        tableEntity.setFullyQualifiedName(TABLE_QUALIFIED_NAME);
        // Set Database Schema
        EntityReference databaseSchema = new EntityReference();
        databaseSchema.setName(SCHEMA_NAME);
        databaseSchema.setFullyQualifiedName(SCHEMA_QUALIFIED_NAME);
        tableEntity.setDatabaseSchema(databaseSchema);

        // Set Column
        List<Column> columns = new ArrayList<>();
        Column columnObject = new Column();
        columnObject.setFullyQualifiedName(COLUMN_QUALIFIED_NAME);
        columnObject.setName(COLUMN_NAME);
        columns.add(columnObject);
        tableEntity.setColumns(columns);
        RangerOpenmetadataEntity entity   = getTableEntity(tableEntity);
        RangerServiceResource  resource = resourceMapper.buildResource(entity);

        // Assert and Test Catalog Resource
        assertCatalogResource(resource);

        //Assert Table Resource
        assertTableResource(resource);

    }
    private RangerOpenmetadataEntity getTableEntity(Table tableEntity) throws Exception {
        RangerOpenmetadataEntity entity = Mockito.mock(RangerOpenmetadataEntity.class);

        Mockito.when(entity.getType()).thenReturn(OpenmetadataTableMapper.OPENMETADATA_ENTITY_TYPE_TABLE);
        Mockito.when(entity.getTableEntityObject()).thenReturn(tableEntity);

        return entity;
    }
    private void assertServiceResource(RangerServiceResource resource) {
        Assert.assertNotNull(resource);
        Assert.assertEquals(SERVICE_NAME, resource.getServiceName());
        Assert.assertNotNull(resource.getResourceElements());
    }

    private void assertCatalogResource(RangerServiceResource resource) {
        assertServiceResource(resource);

        Assert.assertEquals(3, resource.getResourceElements().size());
        Assert.assertTrue(resource.getResourceElements().containsKey(OpenmetadataTableMapper.RANGER_TYPE_TRINO_CATALOG));
        Assert.assertNotNull(resource.getResourceElements().get(OpenmetadataTableMapper.RANGER_TYPE_TRINO_CATALOG).getValues());
        Assert.assertEquals(1, resource.getResourceElements().get(OpenmetadataTableMapper.RANGER_TYPE_TRINO_CATALOG).getValues().size());
        Assert.assertEquals(RANGER_TRINO_CATALOG, resource.getResourceElements().get(OpenmetadataTableMapper.RANGER_TYPE_TRINO_CATALOG).getValues().get(0));
    }
    private void assertTableResource(RangerServiceResource resource) {
        assertServiceResource(resource);

        Assert.assertEquals(3, resource.getResourceElements().size());
        Assert.assertTrue(resource.getResourceElements().containsKey(OpenmetadataTableMapper.RANGER_TYPE_TRINO_CATALOG));
        Assert.assertNotNull(resource.getResourceElements().get(OpenmetadataTableMapper.RANGER_TYPE_TRINO_CATALOG).getValues());
        Assert.assertEquals(1, resource.getResourceElements().get(OpenmetadataTableMapper.RANGER_TYPE_TRINO_CATALOG).getValues().size());
        Assert.assertEquals(RANGER_TRINO_CATALOG, resource.getResourceElements().get(OpenmetadataTableMapper.RANGER_TYPE_TRINO_CATALOG).getValues().get(0));

        Assert.assertEquals(3, resource.getResourceElements().size());
        Assert.assertTrue(resource.getResourceElements().containsKey(OpenmetadataTableMapper.RANGER_TYPE_TRINO_SCHEMA));
        Assert.assertNotNull(resource.getResourceElements().get(OpenmetadataTableMapper.RANGER_TYPE_TRINO_SCHEMA).getValues());
        Assert.assertEquals(1, resource.getResourceElements().get(OpenmetadataTableMapper.RANGER_TYPE_TRINO_SCHEMA).getValues().size());
        Assert.assertEquals(RANGER_TRINO_SCHEMA, resource.getResourceElements().get(OpenmetadataTableMapper.RANGER_TYPE_TRINO_SCHEMA).getValues().get(0));


        Assert.assertTrue(resource.getResourceElements().containsKey(OpenmetadataTableMapper.RANGER_TYPE_TRINO_TABLE));
        Assert.assertNotNull(resource.getResourceElements().get(OpenmetadataTableMapper.RANGER_TYPE_TRINO_TABLE).getValues());
        Assert.assertEquals(1, resource.getResourceElements().get(OpenmetadataTableMapper.RANGER_TYPE_TRINO_TABLE).getValues().size());
        Assert.assertEquals(RANGER_TRINO_TABLE, resource.getResourceElements().get(OpenmetadataTableMapper.RANGER_TYPE_TRINO_TABLE).getValues().get(0));
    }
}
