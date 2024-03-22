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

 package org.apache.ranger.common.view;

import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.entity.XXDBBase;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class ViewBaseBean implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    @JsonIgnore
    private XXDBBase mObj = null;

    /**
     * @return the gjObj
     */
    @JsonIgnore
    public XXDBBase getMObj() {
	return mObj;
    }

    /**
     * @param gjObj
     *            the gjObj to set
     */
    public void setMObj(XXDBBase gjObj) {
	this.mObj = gjObj;
    }

    @JsonIgnore
    public int getMyClassType() {
	return RangerCommonEnums.CLASS_TYPE_NONE;
    }
}
