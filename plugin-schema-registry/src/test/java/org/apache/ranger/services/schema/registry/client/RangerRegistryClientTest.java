package org.apache.ranger.services.schema.registry.client;

import org.apache.ranger.services.schema.registry.client.srclient.SRClient;
import org.apache.ranger.services.schema.registry.client.util.DefaultSRClientForTests;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;


public class RangerRegistryClientTest {


    @Test
    public void connectionTest() {
        SRClient client = new DefaultSRClientForTests();

        RangerRegistryClient rangerRegistryClient =
                new RangerRegistryClient("schema-registry", client);

        HashMap<String, Object> res = rangerRegistryClient.connectionTest();
        assertEquals(res.get("connectivityStatus"), true);
        assertEquals(res.get("message"), "ConnectionTest Successful");
        assertEquals(res.get("description"), "ConnectionTest Successful");
        assertNull(res.get("objectId"));
        assertNull(res.get("fieldName"));


        client = new DefaultSRClientForTests() {
            @Override
            public void testConnection() throws Exception {
                throw new Exception("Cannot connect to the SR server");
            }
        };
        rangerRegistryClient =
                new RangerRegistryClient("schema-registry", client);

        res = rangerRegistryClient.connectionTest();
        String errMessage = "You can still save the repository and start creating "
                + "policies, but you would not be able to use autocomplete for "
                + "resource names. Check server logs for more info.";
        assertEquals(res.get("connectivityStatus"), false);
        assertThat(res.get("message"), is(errMessage));
        assertThat(res.get("description"), is(errMessage));
        assertNull(res.get("objectId"));
        assertNull(res.get("fieldName"));

    }

    @Test
    public void getSchemaGroupList() {
        SRClient client = new DefaultSRClientForTests(){
            @Override
            public List<String> getSchemaGroups() {
                List<String> groups = new ArrayList<>();
                groups.add("testGroup");
                return groups;
            }
        };

        RangerRegistryClient rangerRegistryClient =
                new RangerRegistryClient("schema-registry", client);

        // Empty initialGroups and the list of groups returned by SRClient
        // doesn't contain any groups that starts with 'tesSome'
        List<String> initialGroups = new ArrayList<>();
        List<String> res = rangerRegistryClient.getSchemaGroupList("tesSome", initialGroups);
        assertEquals(0, res.size());

        // Empty initialGroups and the list of groups returned by SRClient
        // contains a group that starts with 'tes'
        initialGroups = new ArrayList<>();
        res = rangerRegistryClient.getSchemaGroupList("tes", initialGroups);
        List<String>  expected = new ArrayList<>();
        expected.add("testGroup");
        assertEquals(1, res.size());
        assertThat(res, is(expected));

        // initialGroups contains one element, list of the groups returned by SRClient
        // contains the same values that are already present in initialGroups
        initialGroups = new ArrayList<>();
        initialGroups.add("testGroup");
        res = rangerRegistryClient.getSchemaGroupList("tes", initialGroups);
        expected = new ArrayList<>();
        expected.add("testGroup");
        assertEquals(1, res.size());
        assertThat(res, is(expected));

        // initialGroups contains one element, list of the groups returned by SRClient
        // contains one element too, that is not equal to the element in initialGroups
        initialGroups = new ArrayList<>();
        initialGroups.add("testGroup2");
        res = rangerRegistryClient.getSchemaGroupList("tes", initialGroups);
        expected = new ArrayList<>();
        expected.add("testGroup2");
        expected.add("testGroup");
        assertEquals(2, res.size());
        assertThat(res, is(expected));

    }

    @Test
    public void getSchemaMetadataList() {
        SRClient client = new DefaultSRClientForTests(){
            @Override
            public List<String> getSchemaNames(List<String> schemaGroup) {
                if(!schemaGroup.contains("Group1")) {
                    return new ArrayList<>();
                }
                List<String> schemas = new ArrayList<>();
                schemas.add("testSchema");
                return schemas;
            }
        };

        RangerRegistryClient rangerRegistryClient =
                new RangerRegistryClient("schema-registry", client);

        List<String> groupList = new ArrayList<>();
        groupList.add("Group1");
        groupList.add("Group2");
        List<String> res = rangerRegistryClient.getSchemaMetadataList("tes", groupList, new ArrayList<>());
        List<String> expected = new ArrayList<>();
        expected.add("testSchema");
        assertEquals(1, res.size());
        assertThat(res, is(expected));

        res = rangerRegistryClient.getSchemaMetadataList("tesSome", groupList, new ArrayList<>());
        assertEquals(0, res.size());
    }

    @Test
    public void getBranchList() {
        SRClient client = new DefaultSRClientForTests(){
            @Override
            public List<String> getSchemaBranches(String schemaMetadataName) {
                if(!schemaMetadataName.equals("Schema1")) {
                    return new ArrayList<>();
                }
                List<String> branches = new ArrayList<>();
                branches.add("testBranch");
                return branches;
            }

            @Override
            public List<String> getSchemaNames(List<String> schemaGroup) {
                if(!schemaGroup.contains("Group1")) {
                    return new ArrayList<>();
                }
                List<String> schemas = new ArrayList<>();
                schemas.add("Schema1");
                return schemas;
            }
        };

        RangerRegistryClient rangerRegistryClient =
                new RangerRegistryClient("schema-registry", client);

        List<String> schemaList = new ArrayList<>();
        schemaList.add("Schema1");
        schemaList.add("Schema2");
        List<String> groups = new ArrayList<>();
        groups.add("Group1");
        List<String> res = rangerRegistryClient.getBranchList("tes", groups, schemaList, new ArrayList<>());
        List<String> expected = new ArrayList<>();
        expected.add("testBranch");
        assertEquals(1, res.size());
        assertThat(res, is(expected));

        res = rangerRegistryClient.getSchemaMetadataList("tesSome", schemaList, new ArrayList<>());
        assertEquals(0, res.size());
    }
}