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

package org.apache.ranger.authorization.knox;

import org.apache.ranger.authorization.knox.KnoxRangerPlugin.KnoxConstants.AccessType;
import org.apache.ranger.authorization.knox.KnoxRangerPlugin.KnoxConstants.PluginConfiguration;
import org.apache.ranger.authorization.knox.KnoxRangerPlugin.KnoxConstants.ResourceName;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.util.List;
import java.util.Set;

public class KnoxRangerPlugin extends RangerBasePlugin {
    boolean initialized;

    public KnoxRangerPlugin() {
        super(PluginConfiguration.ServiceType, PluginConfiguration.AuditApplicationType);
    }

    // must be synchronized so that accidental double init of plugin does not happen .. in case servlet instantiates multiple filters.
    @Override
    public synchronized void init() {
        if (!initialized) {
            // mandatory call to base plugin
            super.init();
            // One time call to register the audit hander with the policy engine.
            super.setResultProcessor(new RangerDefaultAuditHandler(getConfig()));
            initialized = true;
        }
    }

    public static class RequestBuilder {
        String       service;
        String       topology;
        String       user;
        Set<String>  groups;
        String       clientIp;
        String       remoteIp;
        List<String> forwardedAddresses;

        RequestBuilder service(String service) {
            this.service = service;
            return this;
        }

        RequestBuilder topology(String topology) {
            this.topology = topology;
            return this;
        }

        RequestBuilder user(String user) {
            this.user = user;
            return this;
        }

        RequestBuilder groups(Set<String> groups) {
            this.groups = groups;
            return this;
        }

        RequestBuilder clientIp(String clientIp) {
            this.clientIp = clientIp;
            return this;
        }

        RequestBuilder remoteIp(String remoteIp) {
            this.remoteIp = remoteIp;
            return this;
        }

        RequestBuilder forwardedAddresses(List<String> forwardedAddresses) {
            this.forwardedAddresses = forwardedAddresses;
            return this;
        }

        void verifyBuildable() {
            if (topology == null) {
                throw new IllegalStateException("_topology can't be null!");
            }
            if (service == null) {
                throw new IllegalStateException("_service can't be null!");
            }
            if (user == null) {
                throw new IllegalStateException("_user can't be null!");
            }
        }

        RangerAccessRequest build() {
            // build resource
            RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
            resource.setValue(ResourceName.Service, service);
            resource.setValue(ResourceName.Topology, topology);
            // build request
            RangerAccessRequestImpl request = new RangerAccessRequestImpl();
            request.setAction(AccessType.Allow);
            request.setAccessType(AccessType.Allow);
            request.setClientIPAddress(clientIp);
            request.setUser(user);
            request.setUserGroups(groups);
            request.setResource(resource);
            request.setRemoteIPAddress(remoteIp);
            request.setForwardedAddresses(forwardedAddresses);
            return request;
        }
    }

    public static class KnoxConstants {
        // Plugin parameters
        static class PluginConfiguration {
            static final String ServiceType          = "knox";
            static final String AuditApplicationType = "knox";
        }

        // must match the corresponding string used in service definition file
        static class ResourceName {
            static final String Topology = "topology";
            static final String Service  = "service";
        }

        // must match the corresponding string used in service definition file
        static class AccessType {
            static final String Allow = "allow";
        }
    }
}
