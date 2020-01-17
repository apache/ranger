package org.apache.ranger.services.schema.registry.client.util;

import org.apache.ranger.services.schema.registry.client.RangerRegistryClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestRangerRegistryClient extends RangerRegistryClient {
    public TestRangerRegistryClient(String serviceName, Map<String, String> configs) {
        super(serviceName, configs);
    }

    @Override
    public List<String> getSchemaGroupList(String lookupGroupName, List<String> groupList) {
        List<String> res = new ArrayList<>(groupList);
        res.add("testGroup");

        return res;
    }

    @Override
    public List<String> getSchemaMetadataList(String finalSchemaMetadataName,
                                              List<String> schemaGroupList,
                                              List<String> schemaMetadataList) {
        List<String> res = new ArrayList<>(schemaMetadataList);
        res.add("testSchema");

        return res;
    }

    @Override
    public List<String> getBranchList(String lookupBranchName,
                                      List<String> groups,
                                      List<String> schemaList,
                                      List<String> branchList) {
        List<String> res = new ArrayList<>(branchList);
        res.add("testBranch");

        return res;
    }
}
