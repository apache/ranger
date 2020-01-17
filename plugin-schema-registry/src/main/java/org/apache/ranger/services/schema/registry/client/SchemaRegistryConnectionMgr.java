package org.apache.ranger.services.schema.registry.client;

import java.util.HashMap;
import java.util.Map;

public class SchemaRegistryConnectionMgr {

    static public RangerRegistryClient getSchemaRegistryClient(String serviceName,
                                                               Map<String, String> configs) {
        RangerRegistryClient rangerRegistryClient = new RangerRegistryClient(serviceName, configs);
        return rangerRegistryClient;
    }

    /**
     * @param serviceName
     * @param configs
     * @return
     */
    public static HashMap<String, Object> connectionTest(String serviceName,
                                                         Map<String, String> configs) {
        RangerRegistryClient serviceSRClient = getSchemaRegistryClient(serviceName,
                configs);
        return serviceSRClient.connectionTest();
    }
}
