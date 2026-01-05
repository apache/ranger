/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.validation;

import org.apache.ranger.plugin.model.RangerGds.RangerDataShare;
import org.apache.ranger.plugin.model.RangerGds.RangerDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerProject;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;

import java.util.Collection;
import java.util.Set;

public abstract class RangerGdsValidationDataProvider {
    public RangerGdsValidationDataProvider() {
    }

    public abstract Long getServiceId(String name);

    public abstract Long getZoneId(String name);

    public abstract Long getDatasetId(String name);

    public abstract Long getProjectId(String name);

    public abstract Long getDataShareId(String name);

    public abstract Long getUserId(String name);

    public abstract Long getGroupId(String name);

    public abstract Long getRoleId(String name);

    public abstract String getCurrentUserLoginId();

    public abstract boolean isAdminUser();

    public abstract boolean isServiceAdmin(String name);

    public abstract boolean isZoneAdmin(String zoneName);

    public abstract Set<String> getGroupsForUser(String userName);

    public abstract Set<String> getRolesForUser(String userName);

    public abstract Set<String> getRolesForUserAndGroups(String userName, Collection<String> groups);

    public abstract Set<String> getAccessTypes(String serviceName);

    public abstract Set<String> getMaskTypes(String serviceName);

    public abstract RangerDataset getDataset(Long id);

    public abstract RangerProject getProject(Long id);

    public abstract RangerDataShare getDataShare(Long id);

    public abstract Long getSharedResourceId(Long dataShareId, String name);

    public abstract Long getSharedResourceId(Long dataShareId, RangerPolicyResourceSignature signature);
}
