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
 * UserGroupInfo
 */

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class VXGroupUserInfo extends VXDataObject implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    VXGroup      xgroupInfo;
    List<VXUser> xuserInfo;

    public VXGroupUserInfo() {
    }

    public VXGroup getXgroupInfo() {
        return xgroupInfo;
    }

    public void setXgroupInfo(VXGroup xgroupInfo) {
        this.xgroupInfo = xgroupInfo;
    }

    public List<VXUser> getXuserInfo() {
        return xuserInfo;
    }

    public void setXuserInfo(List<VXUser> xuserInfo) {
        this.xuserInfo = xuserInfo;
    }
}
