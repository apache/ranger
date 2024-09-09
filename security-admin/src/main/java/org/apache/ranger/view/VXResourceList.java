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
 * List wrapper class for VXResource
 *
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.ranger.common.view.VList;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class VXResourceList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXResource> vXResources = new ArrayList<VXResource>();

    public VXResourceList() {
	super();
    }

    public VXResourceList(List<VXResource> objList) {
	super(objList);
	this.vXResources = objList;
    }

    /**
     * @return the vXResources
     */
    @JsonProperty("vXResources")
    public List<VXResource> getVXResources() {
	return vXResources;
    }

    /**
     * @param vXResources
     *            the vXResources to set
     */
    @JsonProperty("vXResources")
    public void setVXResources(List<VXResource> vXResources) {
	this.vXResources = vXResources;
    }

    @Override
    public int getListSize() {
	if (vXResources != null) {
	    return vXResources.size();
	}
	return 0;
    }

    @Override
    public List<VXResource> getList() {
	return vXResources;
    }

}
