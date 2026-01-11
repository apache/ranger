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

package org.apache.ranger.view;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.ranger.common.view.VList;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShare;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShareInDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDatasetInProject;
import org.apache.ranger.plugin.model.RangerGds.RangerProject;
import org.apache.ranger.plugin.model.RangerGds.RangerSharedResource;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.util.ArrayList;
import java.util.List;

public class RangerGdsVList {
    @JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerDatasetList extends VList {
        private static final long serialVersionUID = 1L;

        List<RangerDataset> list = new ArrayList<>();

        public RangerDatasetList() {
            super();
        }

        public RangerDatasetList(List<RangerDataset> objList) {
            super(objList);

            this.list = objList;
        }

        @Override
        public int getListSize() {
            return list != null ? list.size() : 0;
        }

        @Override
        public List<RangerDataset> getList() {
            return list;
        }
    }

    @JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerProjectList extends VList {
        private static final long serialVersionUID = 1L;

        List<RangerProject> list = new ArrayList<>();

        public RangerProjectList() {
            super();
        }

        public RangerProjectList(List<RangerProject> objList) {
            super(objList);

            this.list = objList;
        }

        @Override
        public int getListSize() {
            return list != null ? list.size() : 0;
        }

        @Override
        public List<RangerProject> getList() {
            return list;
        }
    }

    @JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerDataShareList extends VList {
        private static final long serialVersionUID = 1L;

        List<RangerDataShare> list = new ArrayList<>();

        public RangerDataShareList() {
            super();
        }

        public RangerDataShareList(List<RangerDataShare> objList) {
            super(objList);

            this.list = objList;
        }

        @Override
        public int getListSize() {
            return list != null ? list.size() : 0;
        }

        @Override
        public List<RangerDataShare> getList() {
            return list;
        }
    }

    @JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerSharedResourceList extends VList {
        private static final long serialVersionUID = 1L;

        List<RangerSharedResource> list = new ArrayList<>();

        public RangerSharedResourceList() {
            super();
        }

        public RangerSharedResourceList(List<RangerSharedResource> objList) {
            super(objList);

            this.list = objList;
        }

        @Override
        public int getListSize() {
            return list != null ? list.size() : 0;
        }

        @Override
        public List<RangerSharedResource> getList() {
            return list;
        }
    }

    @JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerDataShareInDatasetList extends VList {
        private static final long serialVersionUID = 1L;

        List<RangerDataShareInDataset> list = new ArrayList<>();

        public RangerDataShareInDatasetList() {
            super();
        }

        public RangerDataShareInDatasetList(List<RangerDataShareInDataset> objList) {
            super(objList);

            this.list = objList;
        }

        @Override
        public int getListSize() {
            return list != null ? list.size() : 0;
        }

        @Override
        public List<RangerDataShareInDataset> getList() {
            return list;
        }
    }

    @JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RangerDatasetInProjectList extends VList {
        private static final long serialVersionUID = 1L;

        List<RangerDatasetInProject> list = new ArrayList<>();

        public RangerDatasetInProjectList() {
            super();
        }

        public RangerDatasetInProjectList(List<RangerDatasetInProject> objList) {
            super(objList);

            this.list = objList;
        }

        @Override
        public int getListSize() {
            return list != null ? list.size() : 0;
        }

        @Override
        public List<RangerDatasetInProject> getList() {
            return list;
        }
    }
}
