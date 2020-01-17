package org.apache.ranger.services.schema.registry.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.services.schema.registry.client.srclient.DefaultSRClient;
import org.apache.ranger.services.schema.registry.client.srclient.SRClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RangerRegistryClient {
    private static final Log LOG = LogFactory.getLog(RangerRegistryClient.class);

    private SRClient srClient;
    private String serviceName;

    private static final String errMessage = "You can still save the repository and start creating "
            + "policies, but you would not be able to use autocomplete for "
            + "resource names. Check server logs for more info.";

    private static final String successMsg = "ConnectionTest Successful";


    public RangerRegistryClient(String serviceName, Map<String, String> configs) {
        this(serviceName, new DefaultSRClient(configs));
    }

    public RangerRegistryClient(String serviceName, SRClient srClient) {
        this.serviceName = serviceName;
        this.srClient = srClient;
    }

    public HashMap<String, Object> connectionTest() {
        String errMsg = errMessage;
        HashMap<String, Object> responseData = new HashMap<String, Object>();

        try {
            srClient.testConnection();
            // If it doesn't throw exception, then assume the instance is
            // reachable
            BaseClient.generateResponseDataMap(true, successMsg,
                    successMsg, null, null, responseData);
            if(LOG.isDebugEnabled()) {
                LOG.debug("ConnectionTest Successful.");
            }
        } catch (Exception e) {
            LOG.error("Error connecting to SchemaRegistry. schemaRegistryClient=" + this, e);
            BaseClient.generateResponseDataMap(false, errMessage,
                    errMessage, null, null, responseData);
        }

        return responseData;
    }

    public List<String> getSchemaGroupList(String lookupGroupName, List<String> groupList) {
        List<String> res = groupList;
        Collection<String> schemaGroups = srClient.getSchemaGroups();
        schemaGroups.forEach(gName -> {
            if (!res.contains(gName) && gName.contains(lookupGroupName)) {
                res.add(gName);
            }
        });

        return res;
    }

    public List<String> getSchemaMetadataList(String lookupSchemaMetadataName,
                                              List<String> schemaGroupList,
                                              List<String> schemaMetadataList) {
        List<String> res = schemaMetadataList;

        Collection<String> schemas = srClient.getSchemaNames(schemaGroupList);
        schemas.forEach(sName -> {
            if (!res.contains(sName) && sName.contains(lookupSchemaMetadataName)) {
                res.add(sName);
            }
        });

        return res;
    }

    public List<String> getBranchList(String lookupBranchName,
                                      List<String> groupList,
                                      List<String> schemaList,
                                      List<String> branchList) {
        List<String> res = branchList;
        List<String> expandedSchemaList = schemaList.stream().flatMap(
                schemaName -> expandSchemaMetadataNameRegex(groupList, schemaName).stream())
                .collect(Collectors.toList());
        expandedSchemaList.forEach(schemaMetadataName -> {
            Collection<String> branches = srClient.getSchemaBranches(schemaMetadataName);
            branches.forEach(bName -> {
                if (!res.contains(bName) && bName.contains(lookupBranchName)) {
                    res.add(bName);
                }
            });
        });

        return res;
    }

    List<String> expandSchemaMetadataNameRegex(List<String> schemaGroupList, String lookupSchemaMetadataName) {
        List<String> res = new ArrayList<>();

        Collection<String> schemas = srClient.getSchemaNames(schemaGroupList);
        schemas.forEach(sName -> {
            if (sName.matches(lookupSchemaMetadataName)) {
                res.add(sName);
            }
        });

        return res;
    }

    @Override
    public String toString() {
        return "ServiceKafkaClient [serviceName=" + serviceName + "]";
    }

}
