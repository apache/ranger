package org.apache.ranger.services.schema.registry.client.util;

import org.apache.ranger.services.schema.registry.client.srclient.SRClient;

import java.util.ArrayList;
import java.util.List;

public class DefaultSRClientForTests implements SRClient {

    @Override
    public List<String> getSchemaGroups() {
        return new ArrayList<>();
    }

    @Override
    public List<String> getSchemaNames(List<String> schemaGroup) {
        return new ArrayList<>();
    }

    @Override
    public List<String> getSchemaBranches(String schemaMetadataName) {
        return new ArrayList<>();
    }

    @Override
    public void testConnection() throws Exception {

    }
}
