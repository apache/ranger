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
package org.apache.ranger.examples.sampleclient;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.cli.*;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SampleClient {
    private static final Logger LOG = LoggerFactory.getLogger(SampleClient.class);


    @SuppressWarnings("static-access")
    public static void main(String[] args) throws RangerServiceException {
        Gson gsonBuilder = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").setPrettyPrinting().create();
        Options options  = new Options();

        Option host = OptionBuilder.hasArgs(1).isRequired().withLongOpt("host").withDescription("hostname").create('h');
        Option user = OptionBuilder.hasArgs(1).isRequired().withLongOpt("user").withDescription("username").create('u');
        Option pass = OptionBuilder.hasArgs(1).isRequired().withLongOpt("pass").withDescription("password").create('p');

        options.addOption(host);
        options.addOption(user);
        options.addOption(pass);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        String hostName = cmd.getOptionValue('h');
        String userName = cmd.getOptionValue('u');
        String password = cmd.getOptionValue('p');

        RangerClient rangerClient = new RangerClient(hostName, userName, password);

        String serviceDefName     = "sampleServiceDef";
        String serviceName        = "sampleService";
        String policyName         = "samplePolicy";
        String roleName           = "sampleRole";
        Map<String,String> filter = Collections.emptyMap();


        /*
        Create a new Service Definition
         */

        RangerServiceDef.RangerServiceConfigDef config = new RangerServiceDef.RangerServiceConfigDef();
        config.setItemId(1L);
        config.setName("sampleConfig");
        config.setType("string");
        List<RangerServiceDef.RangerServiceConfigDef> configs = Collections.singletonList(config);

        RangerServiceDef.RangerAccessTypeDef accessType = new RangerServiceDef.RangerAccessTypeDef();
        accessType.setItemId(1L);
        accessType.setName("sampleAccess");
        List<RangerServiceDef.RangerAccessTypeDef> accessTypes = Collections.singletonList(accessType);

        RangerServiceDef.RangerResourceDef resourceDef = new RangerServiceDef.RangerResourceDef();
        resourceDef.setItemId(1L);
        resourceDef.setName("root");
        resourceDef.setType("string");
        List<RangerServiceDef.RangerResourceDef> resourceDefs = Collections.singletonList(resourceDef);

        RangerServiceDef serviceDef = new RangerServiceDef();
        serviceDef.setName(serviceDefName);
        serviceDef.setConfigs(configs);
        serviceDef.setAccessTypes(accessTypes);
        serviceDef.setResources(resourceDefs);

        RangerServiceDef createdServiceDef = rangerClient.createServiceDef(serviceDef);
        LOG.info("New Service Definition created successfully {}", gsonBuilder.toJson(createdServiceDef));

        /*
        Create a new Service
         */
        RangerService service = new RangerService();
        service.setType(serviceDefName);
        service.setName(serviceName);

        RangerService createdService = rangerClient.createService(service);
        LOG.info("New Service created successfully {}", gsonBuilder.toJson(createdService));

        /*
        Policy Management
         */


        /*
        Create a new Policy
         */
        Map<String, RangerPolicy.RangerPolicyResource> resource = Collections.singletonMap(
                "root", new RangerPolicy.RangerPolicyResource(Collections.singletonList("/path/to/sample/resource"),false,false));
        RangerPolicy policy = new RangerPolicy();
        policy.setService(serviceName);
        policy.setName(policyName);
        policy.setResources(resource);

        RangerPolicy createdPolicy = rangerClient.createPolicy(policy);
        LOG.info("New Policy created successfully {}", gsonBuilder.toJson(createdPolicy));

        /*
        Get a policy by name
         */
        RangerPolicy fetchedPolicy = rangerClient.getPolicy(serviceName, policyName);
        LOG.info("Policy: {} fetched {}", policyName, gsonBuilder.toJson(fetchedPolicy));


        /*
        Delete a policy
         */
        rangerClient.deletePolicy(serviceName, policyName);
        LOG.info("Policy {} successfully deleted", policyName);


        /*
        Delete a Service
         */
        rangerClient.deleteService(serviceName);
        LOG.info("Service {} successfully deleted", serviceName);


        /*
        Delete a Service Definition
         */
        rangerClient.deleteServiceDef(serviceDefName);
        LOG.info("Service Definition {} successfully deleted", serviceDefName);


        /*
        Role Management
         */

        /*
        Create a role in Ranger
         */
        RangerRole sampleRole = new RangerRole();
        sampleRole.setName(roleName);
        sampleRole.setDescription("Sample Role");
        sampleRole.setUsers(Collections.singletonList(new RangerRole.RoleMember(null,true)));
        sampleRole = rangerClient.createRole(serviceName, sampleRole);
        LOG.info("New Role successfully created {}", gsonBuilder.toJson(sampleRole));

        /*
        Update a role in Ranger
         */
        sampleRole.setDescription("Updated Sample Role");
        RangerRole updatedRole = rangerClient.updateRole(sampleRole.getId(), sampleRole);
        LOG.info("Role {} successfully updated {}", roleName, gsonBuilder.toJson(updatedRole));

        /*
        Get all roles in Ranger
         */
        List<RangerRole> allRoles = rangerClient.findRoles(filter);
        LOG.info("List of Roles {}", gsonBuilder.toJson(allRoles));

        /*
        Delete a role in Ranger
         */
        rangerClient.deleteRole(roleName, userName, serviceName);
        LOG.info("Role {} successfully deleted", roleName);
    }
}
