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

 package org.apache.ranger.admin.client;


import org.apache.ranger.plugin.model.RangerTaggedResource;
import org.apache.ranger.plugin.model.RangerTaggedResourceKey;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.TagServiceResources;

import java.util.List;


public interface RangerAdminClient {
	void init(String serviceName, String appId, String configPropertyPrefix);

	ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion) throws Exception;

	void grantAccess(GrantRevokeRequest request) throws Exception;

	void revokeAccess(GrantRevokeRequest request) throws Exception;

	TagServiceResources getTaggedResources(String tagServiceName, String componentType, Long lastTimestamp) throws Exception;

	List<String> getTagNames(String tagServiceName, String componentType, String tagNamePattern) throws Exception;

	TagServiceResources getAllTaggedResources() throws Exception;

	List<RangerTaggedResource> setResources(List<RangerTaggedResourceKey> keys, List<RangerTaggedResource.RangerResourceTag> tags) throws Exception;

	RangerTaggedResource setResource(RangerTaggedResourceKey key, List<RangerTaggedResource.RangerResourceTag> tags) throws Exception;

	RangerTaggedResource updateResourceTags(RangerTaggedResourceKey key, List<RangerTaggedResource.RangerResourceTag> tagsToAdd,
							  List<RangerTaggedResource.RangerResourceTag> tagsToDelete) throws Exception;

}
