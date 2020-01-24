package org.apache.ranger.services.schema.registry.client.srclient;

import java.util.List;

public interface SRClient {
    List<String> getSchemaGroups();
    List<String> getSchemaNames(List<String> schemaGroup);
    List<String> getSchemaBranches(String schemaMetadataName);
    void checkConnection() throws Exception;
}

