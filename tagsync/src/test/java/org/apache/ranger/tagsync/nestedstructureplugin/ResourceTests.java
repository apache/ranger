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

package org.apache.ranger.tagsync.nestedstructureplugin;

import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.tagsync.source.atlasrest.RangerAtlasEntity;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.apache.ranger.tagsync.nestedstructureplugin.AtlasNestedStructureResourceMapper.QUALIFIED_NAME_DELIMITER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ResourceTests {
    private AtlasNestedStructureResourceMapper mapper = new AtlasNestedStructureResourceMapper();

    @Test
    public void test_ResourceParseFieldName() {
        String   resourceStr = "json_object.foo.v1#partner";
        String[] resources   = resourceStr.split(QUALIFIED_NAME_DELIMITER);
        String   schemaName  = resources.length > 0 ? resources[0] : null;
        String   fieldName   = resources.length > 1 ? resources[1] : null;

        assertEquals("json_object.foo.v1", schemaName, "schemaName does not match expected value");
        assertEquals("partner", fieldName, "fieldName does not match expected value");
        System.out.println(schemaName);
        System.out.println(fieldName);
    }

    @Test
    public void test_ResourceParseSchemaName() {
        String   resourceStr = "json_object.foo.v1";
        String[] resources   = resourceStr.split(QUALIFIED_NAME_DELIMITER);
        String   schemaName  = resources.length > 0 ? resources[0] : null;
        String   fieldName   = resources.length > 1 ? resources[1] : null;

        assertEquals(schemaName, resourceStr, "schemaName does not match expected value");
        assertNull(fieldName, "fieldName does not match expected value");
        System.out.println(schemaName);
        System.out.println(fieldName);
    }

    @Test
    public void test_RangerEntityJsonField() {
        String typeName = "json_field";
        String guid     = "0265354542434ff-aewra7297dc";

        try {
            Map<String, Object>   attributes = Collections.singletonMap("qualifiedName", "json_object.foo.v1#channel");
            RangerAtlasEntity     entity     = new RangerAtlasEntity(typeName, guid, attributes);
            RangerServiceResource resource   = mapper.buildResource(entity);

            assertTrue(resource.getResourceElements().size() > 0, "Resource elements list is empty");
            assertEquals(2, resource.getResourceElements().size(), "Resource elements list size does not match expected");
            assertNotNull(resource.getResourceElements().get("schema"), "Resource element missing value for schema");
            assertEquals(Collections.singletonList("json_object.foo.v1"), resource.getResourceElements().get("schema").getValues(), "Resource element schema value does not match");
            assertNotNull(resource.getResourceElements().get("field"), "Resource element missing value for field");
            assertEquals(Collections.singletonList("channel"), resource.getResourceElements().get("field").getValues(), "Resource element field value does not match");
            assertEquals("null_nestedstructure", resource.getServiceName(), "serviceName does not match expected value");
        } catch (Exception e) {
            e.printStackTrace();
            fail("An error occurred while processing resource");
        }

        // qualifiedName containing clusterName
        try {
            Map<String, Object>   attributes = Collections.singletonMap("qualifiedName", "json_object.foo.v1#channel@dev");
            RangerAtlasEntity     entity     = new RangerAtlasEntity(typeName, guid, attributes);
            RangerServiceResource resource   = mapper.buildResource(entity);

            assertTrue(resource.getResourceElements().size() > 0, "Resource elements list is empty");
            assertEquals(2, resource.getResourceElements().size(), "Resource elements list size does not match expected");
            assertNotNull(resource.getResourceElements().get("schema"), "Resource element missing value for schema");
            assertEquals(Collections.singletonList("json_object.foo.v1"), resource.getResourceElements().get("schema").getValues(), "Resource element schema value does not match");
            assertNotNull(resource.getResourceElements().get("field"), "Resource element missing value for field");
            assertEquals(Collections.singletonList("channel"), resource.getResourceElements().get("field").getValues(), "Resource element field value does not match");
            assertEquals("dev_nestedstructure", resource.getServiceName(), "serviceName does not match expected value");
        } catch (Exception e) {
            e.printStackTrace();
            fail("An error occurred while processing resource");
        }
    }

    @Test
    public void test_RangerEntityJsonObject() {
        String typeName = "json_object";
        String guid     = "9fsdd-sfsrsag-dasd-3fa97";

        try {
            Map<String, Object>   attributes = Collections.singletonMap("qualifiedName", "json_object.foo.v1");
            RangerAtlasEntity     entity     = new RangerAtlasEntity(typeName, guid, attributes);
            RangerServiceResource resource   = mapper.buildResource(entity);

            assertTrue(resource.getResourceElements().size() > 0, "Resource elements list is empty");
            assertEquals(1, resource.getResourceElements().size(), "Resource elements list size does not match expected");
            assertNotNull(resource.getResourceElements().get("schema"), "Resource element missing value for schema");
            assertEquals(Collections.singletonList("json_object.foo.v1"), resource.getResourceElements().get("schema").getValues(), "Resource element schema value does not match");
            assertEquals("null_nestedstructure", resource.getServiceName(), "serviceName does not match expected value");
        } catch (Exception e) {
            e.printStackTrace();
            fail("An error occurred while processing resource");
        }

        // qualifiedName containing clusterName
        try {
            Map<String, Object>   attributes = Collections.singletonMap("qualifiedName", "json_object.foo.v1@dev");
            RangerAtlasEntity     entity     = new RangerAtlasEntity(typeName, guid, attributes);
            RangerServiceResource resource   = mapper.buildResource(entity);

            assertTrue(resource.getResourceElements().size() > 0, "Resource elements list is empty");
            assertEquals(1, resource.getResourceElements().size(), "Resource elements list size does not match expected");
            assertNotNull(resource.getResourceElements().get("schema"), "Resource element missing value for schema");
            assertEquals(Collections.singletonList("json_object.foo.v1"), resource.getResourceElements().get("schema").getValues(), "Resource element schema value does not match");
            assertEquals("dev_nestedstructure", resource.getServiceName(), "serviceName does not match expected value");
        } catch (Exception e) {
            e.printStackTrace();
            fail("An error occurred while processing resource");
        }
    }
}
