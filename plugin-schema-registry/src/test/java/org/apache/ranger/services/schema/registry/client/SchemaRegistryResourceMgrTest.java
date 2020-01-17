package org.apache.ranger.services.schema.registry.client;

import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.schema.registry.client.util.TestRangerRegistryClient;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SchemaRegistryResourceMgrTest {

    @Test
    public void getSchemaRegistryResources() throws Exception {
        String serviceName = "schema-registry";
        Map<String, String> configs = new HashMap<>();
        configs.put("schema.registry.url", "http://dummyname:8081");
        RangerRegistryClient client = new TestRangerRegistryClient("schema-registry", configs);


        ResourceLookupContext lookupContext = new ResourceLookupContext();
        lookupContext.setResources(new HashMap<>());
        List<String> groups = new ArrayList<>(), schemas = new ArrayList<>(), branches = new ArrayList<>();
        groups.add("Group1");
        schemas.add("Schema1");
        branches.add("Branch1");

        lookupContext.getResources().put("schema-group", groups);
        lookupContext.getResources().put("schema-metadata", schemas);
        lookupContext.getResources().put("schema-branch", branches);

        lookupContext.setResourceName("schema-group");
        lookupContext.setUserInput("test");
        List<String> res = SchemaRegistryResourceMgr.getSchemaRegistryResources(serviceName,
                configs,
                lookupContext,
                client);
        List<String> expected = new ArrayList<>();
        expected.add("Group1"); expected.add("testGroup");
        assertThat(res, is(expected));

        lookupContext.setResourceName("schema-metadata");
        lookupContext.setUserInput("testS");
        res = SchemaRegistryResourceMgr.getSchemaRegistryResources(serviceName,
                configs,
                lookupContext,
                client);
        expected = new ArrayList<>();
        expected.add("Schema1"); expected.add("testSchema");
        assertThat(res, is(expected));

        lookupContext.setResourceName("schema-branch");
        lookupContext.setUserInput("testB");
        res = SchemaRegistryResourceMgr.getSchemaRegistryResources(serviceName,
                configs,
                lookupContext,
                client);
        expected = new ArrayList<>();
        expected.add("Branch1"); expected.add("testBranch");
        assertThat(res, is(expected));

        lookupContext.setResourceName("schema-version");
        lookupContext.setUserInput("*");
        res = SchemaRegistryResourceMgr.getSchemaRegistryResources(serviceName,
                configs,
                lookupContext,
                client);
        expected = new ArrayList<>();
        expected.add("*");
        assertThat(res, is(expected));

        lookupContext.setResourceName("serde");
        res = SchemaRegistryResourceMgr.getSchemaRegistryResources(serviceName,
                configs,
                lookupContext,
                client);
        assertThat(res, is(expected));

        lookupContext.setResourceName("none-sb");
        res = SchemaRegistryResourceMgr.getSchemaRegistryResources(serviceName,
                configs,
                lookupContext,
                client);
        assertThat(res, is(expected));

        lookupContext.setResourceName("none-sv");
        res = SchemaRegistryResourceMgr.getSchemaRegistryResources(serviceName,
                configs,
                lookupContext,
                client);
        assertThat(res, is(expected));

        lookupContext.setResourceName("registry-service");
        res = SchemaRegistryResourceMgr.getSchemaRegistryResources(serviceName,
                configs,
                lookupContext,
                client);
        assertThat(res, is(expected));

    }
}