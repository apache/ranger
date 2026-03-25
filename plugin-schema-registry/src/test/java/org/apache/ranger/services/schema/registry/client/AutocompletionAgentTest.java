/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.services.schema.registry.client;

import org.apache.ranger.services.schema.registry.client.connection.ISchemaRegistryClient;
import org.apache.ranger.services.schema.registry.client.util.DefaultSchemaRegistryClientForTesting;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class AutocompletionAgentTest {
    @Test
    public void connectionTest() {
        ISchemaRegistryClient client            = new DefaultSchemaRegistryClientForTesting();
        AutocompletionAgent autocompletionAgent = new AutocompletionAgent("schema-registry", client);

        HashMap<String, Object> res = autocompletionAgent.connectionTest();
        Assertions.assertEquals(true, res.get("connectivityStatus"));
        Assertions.assertEquals("ConnectionTest Successful", res.get("message"));
        Assertions.assertEquals("ConnectionTest Successful", res.get("description"));
        Assertions.assertNull(res.get("objectId"));
        Assertions.assertNull(res.get("fieldName"));

        client              = new DefaultSchemaRegistryClientForTesting() {
            public void checkConnection() throws Exception {
                throw new Exception("Cannot connect to the SR server");
            }
        };
        autocompletionAgent = new AutocompletionAgent("schema-registry", client);

        res = autocompletionAgent.connectionTest();
        String errMessage = "You can still save the repository and start creating policies, but you would not be able to use autocomplete for resource names. Check server logs for more info.";
        Assertions.assertEquals(false, res.get("connectivityStatus"));
        assertThat(res.get("message"), is(errMessage));
        assertThat(res.get("description"), is(errMessage));
        Assertions.assertNull(res.get("objectId"));
        Assertions.assertNull(res.get("fieldName"));
    }

    @Test
    public void getSchemaGroupList() {
        ISchemaRegistryClient client = new DefaultSchemaRegistryClientForTesting() {
            public List<String> getSchemaGroups() {
                List<String> groups = new ArrayList<>();
                groups.add("testGroup");
                return groups;
            }
        };

        AutocompletionAgent autocompletionAgent = new AutocompletionAgent("schema-registry", client);

        // Empty initialGroups and the list of groups returned by ISchemaRegistryClient
        // doesn't contain any groups that starts with 'tesSome'
        List<String> initialGroups = new ArrayList<>();
        List<String> res           = autocompletionAgent.getSchemaGroupList("tesSome", initialGroups);
        Assertions.assertEquals(0, res.size());

        // Empty initialGroups and the list of groups returned by ISchemaRegistryClient
        // contains a group that starts with 'tes'
        initialGroups = new ArrayList<>();
        res           = autocompletionAgent.getSchemaGroupList("tes", initialGroups);
        List<String> expected = new ArrayList<>();
        expected.add("testGroup");
        Assertions.assertEquals(1, res.size());
        assertThat(res, is(expected));

        // initialGroups contains one element, list of the groups returned by ISchemaRegistryClient
        // contains the same values that are already present in initialGroups
        initialGroups = new ArrayList<>();
        initialGroups.add("testGroup");
        res      = autocompletionAgent.getSchemaGroupList("tes", initialGroups);
        expected = new ArrayList<>();
        expected.add("testGroup");
        Assertions.assertEquals(1, res.size());
        assertThat(res, is(expected));

        // initialGroups contains one element, list of the groups returned by ISchemaRegistryClient
        // contains one element too, that is not equal to the element in initialGroups
        initialGroups = new ArrayList<>();
        initialGroups.add("testGroup2");
        res      = autocompletionAgent.getSchemaGroupList("tes", initialGroups);
        expected = new ArrayList<>();
        expected.add("testGroup2");
        expected.add("testGroup");
        Assertions.assertEquals(2, res.size());
        assertThat(res, is(expected));
    }

    @Test
    public void getSchemaMetadataList() {
        ISchemaRegistryClient client = new DefaultSchemaRegistryClientForTesting() {
            public List<String> getSchemaNames(List<String> schemaGroup) {
                if (!schemaGroup.contains("Group1")) {
                    return new ArrayList<>();
                }
                List<String> schemas = new ArrayList<>();
                schemas.add("testSchema");
                return schemas;
            }
        };

        AutocompletionAgent autocompletionAgent = new AutocompletionAgent("schema-registry", client);

        List<String> groupList = new ArrayList<>();
        groupList.add("Group1");
        groupList.add("Group2");
        List<String> res      = autocompletionAgent.getSchemaMetadataList("tes", groupList, new ArrayList<>());
        List<String> expected = new ArrayList<>();
        expected.add("testSchema");
        Assertions.assertEquals(1, res.size());
        assertThat(res, is(expected));

        res = autocompletionAgent.getSchemaMetadataList("tesSome", groupList, new ArrayList<>());
        Assertions.assertEquals(0, res.size());
    }

    @Test
    public void getBranchList() {
        ISchemaRegistryClient client = new DefaultSchemaRegistryClientForTesting() {
            public List<String> getSchemaNames(List<String> schemaGroup) {
                if (!schemaGroup.contains("Group1")) {
                    return new ArrayList<>();
                }
                List<String> schemas = new ArrayList<>();
                schemas.add("Schema1");
                return schemas;
            }

            public List<String> getSchemaBranches(String schemaMetadataName) {
                if (!schemaMetadataName.equals("Schema1")) {
                    return new ArrayList<>();
                }
                List<String> branches = new ArrayList<>();
                branches.add("testBranch");
                return branches;
            }
        };

        AutocompletionAgent autocompletionAgent = new AutocompletionAgent("schema-registry", client);

        List<String> schemaList = new ArrayList<>();
        schemaList.add("Schema1");
        schemaList.add("Schema2");
        List<String> groups = new ArrayList<>();
        groups.add("Group1");
        List<String> res      = autocompletionAgent.getBranchList("tes", groups, schemaList, new ArrayList<>());
        List<String> expected = new ArrayList<>();
        expected.add("testBranch");
        Assertions.assertEquals(1, res.size());
        assertThat(res, is(expected));

        res = autocompletionAgent.getSchemaMetadataList("tesSome", schemaList, new ArrayList<>());
        Assertions.assertEquals(0, res.size());
    }

    @Test
    void testValidatePattern_validAlphanumeric() {
        ISchemaRegistryClient client = new DefaultSchemaRegistryClientForTesting() {
            public List<String> getSchemaNames(List<String> schemaGroup) {
                List<String> schemas = new ArrayList<>();
                schemas.add("mySchema123");
                return schemas;
            }
        };
        AutocompletionAgent agent = new AutocompletionAgent("test", client);
        List<String> groups = new ArrayList<>();
        groups.add("testGroup");
        List<String> result = agent.expandSchemaMetadataNameRegex(groups, "mySchema123");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("mySchema123", result.get(0));
    }

    @Test
    void testValidatePattern_validWildcards() {
        ISchemaRegistryClient client = new DefaultSchemaRegistryClientForTesting() {
            public List<String> getSchemaNames(List<String> schemaGroup) {
                List<String> schemas = new ArrayList<>();
                schemas.add("mySchema123");
                schemas.add("testSchema");
                return schemas;
            }
        };
        AutocompletionAgent agent = new AutocompletionAgent("test", client);
        List<String> groups = new ArrayList<>();
        groups.add("testGroup");
        List<String> result = agent.expandSchemaMetadataNameRegex(groups, "my*");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("mySchema123", result.get(0));
    }

    @Test
    void testValidatePattern_rejectsReDoSPattern() {
        ISchemaRegistryClient client = new DefaultSchemaRegistryClientForTesting();
        AutocompletionAgent agent = new AutocompletionAgent("test", client);
        List<String> groups = new ArrayList<>();
        groups.add("testGroup");
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            agent.expandSchemaMetadataNameRegex(groups, "(a+)+");
        });
    }

    @Test
    void testValidatePattern_rejectsComplexRegex() {
        ISchemaRegistryClient client = new DefaultSchemaRegistryClientForTesting();
        AutocompletionAgent agent = new AutocompletionAgent("test", client);
        List<String> groups = new ArrayList<>();
        groups.add("testGroup");
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            agent.expandSchemaMetadataNameRegex(groups, "test{1,5}");
        });
    }

    @Test
    void testValidatePattern_rejectsInjectionAttempt() {
        ISchemaRegistryClient client = new DefaultSchemaRegistryClientForTesting();
        AutocompletionAgent agent = new AutocompletionAgent("test", client);
        List<String> groups = new ArrayList<>();
        groups.add("testGroup");
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            agent.expandSchemaMetadataNameRegex(groups, "test'; DROP TABLE users--");
        });
    }

    @Test
    void testConvertWildcardToRegex_asterisk() {
        ISchemaRegistryClient client = new DefaultSchemaRegistryClientForTesting() {
            public List<String> getSchemaNames(List<String> schemaGroup) {
                List<String> schemas = new ArrayList<>();
                schemas.add("testSchema");
                schemas.add("prodSchema");
                return schemas;
            }
        };
        AutocompletionAgent agent = new AutocompletionAgent("test", client);
        List<String> groups = new ArrayList<>();
        groups.add("testGroup");
        List<String> result = agent.expandSchemaMetadataNameRegex(groups, "test*");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("testSchema", result.get(0));
    }

    @Test
    void testConvertWildcardToRegex_questionMark() {
        ISchemaRegistryClient client = new DefaultSchemaRegistryClientForTesting() {
            public List<String> getSchemaNames(List<String> schemaGroup) {
                List<String> schemas = new ArrayList<>();
                schemas.add("schema1");
                schemas.add("schema12");
                return schemas;
            }
        };
        AutocompletionAgent agent = new AutocompletionAgent("test", client);
        List<String> groups = new ArrayList<>();
        groups.add("testGroup");
        List<String> result = agent.expandSchemaMetadataNameRegex(groups, "schema?");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("schema1", result.get(0));
    }
}
