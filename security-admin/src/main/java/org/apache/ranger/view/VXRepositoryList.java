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

package org.apache.ranger.view;

/**
 * List wrapper class for VXRepository
 */

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.ranger.common.view.VList;

import java.util.ArrayList;
import java.util.List;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class VXRepositoryList extends VList {
    private static final long serialVersionUID = 1L;

    List<VXRepository> vXRepositories = new ArrayList<>();

    public VXRepositoryList() {
        super();
    }

    public VXRepositoryList(List<VXRepository> objList) {
        super(objList);
        this.vXRepositories = objList;
    }

    /**
     * @return the vXRepositories
     */
    @JsonProperty("vXRepositories")
    public List<VXRepository> getVXRepositories() {
        return vXRepositories;
    }

    /**
     * @param vXRepositories the vXRepositories to set
     */
    @JsonProperty("vXRepositories")
    public void setVXRepositories(List<VXRepository> vXRepositories) {
        this.vXRepositories = vXRepositories;
    }

    @Override
    public int getListSize() {
        if (vXRepositories != null) {
            return vXRepositories.size();
        }
        return 0;
    }

    @Override
    public List<VXRepository> getList() {
        return vXRepositories;
    }
}
