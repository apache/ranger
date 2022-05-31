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

package org.apache.ranger.authorization.atlas.authorizer;

import org.apache.atlas.authorize.AtlasAccessorResponse;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.ranger.authorization.atlas.authorizer.RangerAtlasAuthorizer.CLASSIFICATION_PRIVILEGES;
import static org.apache.ranger.services.atlas.RangerServiceAtlas.RESOURCE_CLASSIFICATION;
import static org.apache.ranger.services.atlas.RangerServiceAtlas.RESOURCE_ENTITY_BUSINESS_METADATA;
import static org.apache.ranger.services.atlas.RangerServiceAtlas.RESOURCE_ENTITY_ID;
import static org.apache.ranger.services.atlas.RangerServiceAtlas.RESOURCE_ENTITY_LABEL;
import static org.apache.ranger.services.atlas.RangerServiceAtlas.RESOURCE_ENTITY_OWNER;
import static org.apache.ranger.services.atlas.RangerServiceAtlas.RESOURCE_ENTITY_TYPE;


public class RangerAtlasAuthorizerUtil {

    static void toRangerRequest(AtlasEntityAccessRequest request, RangerAccessRequestImpl rangerRequest, RangerAccessResourceImpl rangerResource){

        final String                   action         = request.getAction() != null ? request.getAction().getType() : null;
        final Set<String> entityTypes                 = request.getEntityTypeAndAllSuperTypes();
        final String                   entityId       = request.getEntityId();
        final String                   classification = request.getClassification() != null ? request.getClassification().getTypeName() : null;
        final String                   ownerUser      = request.getEntity() != null ? (String) request.getEntity().getAttribute(RESOURCE_ENTITY_OWNER) : null;

        rangerResource.setValue(RESOURCE_ENTITY_TYPE, entityTypes);
        rangerResource.setValue(RESOURCE_ENTITY_ID, entityId);
        rangerResource.setOwnerUser(ownerUser);
        rangerRequest.setAccessType(action);
        rangerRequest.setAction(action);
        rangerRequest.setUser(request.getUser());
        rangerRequest.setUserGroups(request.getUserGroups());
        rangerRequest.setClientIPAddress(request.getClientIPAddress());
        rangerRequest.setAccessTime(request.getAccessTime());
        rangerRequest.setResource(rangerResource);
        rangerRequest.setForwardedAddresses(request.getForwardedAddresses());
        rangerRequest.setRemoteIPAddress(request.getRemoteIPAddress());

        if (AtlasPrivilege.ENTITY_ADD_LABEL.equals(request.getAction()) || AtlasPrivilege.ENTITY_REMOVE_LABEL.equals(request.getAction())) {
            rangerResource.setValue(RESOURCE_ENTITY_LABEL, request.getLabel());
        } else if (AtlasPrivilege.ENTITY_UPDATE_BUSINESS_METADATA.equals(request.getAction())) {
            rangerResource.setValue(RESOURCE_ENTITY_BUSINESS_METADATA, request.getBusinessMetadata());
        } else if (StringUtils.isNotEmpty(classification) && CLASSIFICATION_PRIVILEGES.contains(request.getAction())) {
            rangerResource.setValue(RESOURCE_CLASSIFICATION, request.getClassificationTypeAndAllSuperTypes(classification));
        }
    }

    static void collectAccessors(RangerAccessResult result, AtlasAccessorResponse response) {
        if (result != null && CollectionUtils.isNotEmpty(result.getMatchedItems())) {

            result.getMatchedItems().forEach(x -> {
                response.getUsers().addAll(x.getUsers());
                response.getRoles().addAll(x.getRoles());
                response.getGroups().addAll(x.getGroups());
            });
        }
    }

    static void collectAccessors(RangerAccessResult resultEnd1, RangerAccessResult resultEnd2, AtlasAccessorResponse accessorResponse) {

        if (resultEnd2 == null || CollectionUtils.isEmpty(resultEnd2.getMatchedItems()))  {
            return;
        }

        final List<String> usersEnd1 = new ArrayList<>();
        final List<String> rolesEnd1 = new ArrayList<>();
        final List<String> groupsEnd1 = new ArrayList<>();

        final List<String> usersEnd2 = new ArrayList<>();
        final List<String> rolesEnd2 = new ArrayList<>();
        final List<String> groupsEnd2 = new ArrayList<>();

        // Collect lists of accessors for both results
        resultEnd1.getMatchedItems().forEach(x -> {
            usersEnd1.addAll(x.getUsers());
            rolesEnd1.addAll(x.getRoles());
            groupsEnd1.addAll(x.getGroups());
        });

        resultEnd2.getMatchedItems().forEach(x -> {
            usersEnd2.addAll(x.getUsers());
            rolesEnd2.addAll(x.getRoles());
            groupsEnd2.addAll(x.getGroups());
        });

        // Retain only common accessors
        usersEnd1.retainAll(usersEnd2);
        rolesEnd1.retainAll(rolesEnd2);
        groupsEnd1.retainAll(groupsEnd2);

        // add accessors to the response
        accessorResponse.getUsers().addAll(usersEnd1);
        accessorResponse.getRoles().addAll(rolesEnd1);
        accessorResponse.getGroups().addAll(groupsEnd1);
    }

    static boolean hasAccessors(RangerAccessResult result) {
        if (result == null) {
            return false;
        }

        for (RangerPolicy.RangerPolicyItem item : result.getMatchedItems()) {
            if (CollectionUtils.isNotEmpty(item.getUsers()) || CollectionUtils.isNotEmpty(item.getRoles()) && CollectionUtils.isNotEmpty(item.getGroups())) {
                return true;
            }
        }
        return false;
    }
}
