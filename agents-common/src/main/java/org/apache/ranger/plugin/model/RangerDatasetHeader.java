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

package org.apache.ranger.plugin.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.util.Map;

public class RangerDatasetHeader {
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerDatasetHeaderInfo extends RangerBaseModelObject implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String                                   name;
        private Map<RangerGds.GdsShareStatus, Long>      dataSharesCountByStatus;
        private Map<RangerPrincipal.PrincipalType, Long> principalsCountByType;
        private Long                                     projectsCount;
        private String                                   permissionForCaller;
        private Long                                     resourceCount;

        public RangerDatasetHeaderInfo() {
            super();
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Map<RangerGds.GdsShareStatus, Long> getDataSharesCountByStatus() {
            return dataSharesCountByStatus;
        }

        public void setDataSharesCountByStatus(Map<RangerGds.GdsShareStatus, Long> dataSharesCountByStatus) {
            this.dataSharesCountByStatus = dataSharesCountByStatus;
        }

        public Map<RangerPrincipal.PrincipalType, Long> getPrincipalsCountByType() {
            return principalsCountByType;
        }

        public void setPrincipalsCountByType(Map<RangerPrincipal.PrincipalType, Long> principalsCountByType) {
            this.principalsCountByType = principalsCountByType;
        }

        public Long getProjectsCount() {
            return projectsCount;
        }

        public void setProjectsCount(Long projectsCount) {
            this.projectsCount = projectsCount;
        }

        public String getPermissionForCaller() {
            return permissionForCaller;
        }

        public void setPermissionForCaller(String permissionForCaller) {
            this.permissionForCaller = permissionForCaller;
        }

        public Long getResourceCount() {
            return resourceCount;
        }

        public void setResourceCount(Long resourceCount) {
            this.resourceCount = resourceCount;
        }
    }
}
