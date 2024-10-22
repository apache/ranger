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

package org.apache.ranger.plugin.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;

import static org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class RangerServiceHeaderInfo extends RangerBaseModelObject implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private String  name;
    private String  displayName;
    private String  type;
    private Boolean isTagService;

    public RangerServiceHeaderInfo() {
        super();
        setId(-1L);
        setName("");
        setIsTagService(false);
    }

    public RangerServiceHeaderInfo(Long id, String name, boolean isTagService) {
        super();
        setId(id);
        setName(name);
        setIsTagService(isTagService);
    }

    public RangerServiceHeaderInfo(Long id, String name, String displayName, String type) {
        super();
        setId(id);
        setName(name);
        setDisplayName(displayName);
        setType(type);
        setIsTagService(EMBEDDED_SERVICEDEF_TAG_NAME.equals(type));
    }

    public RangerServiceHeaderInfo(Long id, String name, String displayName, String type, Boolean isEnabled) {
        super();
        setId(id);
        setName(name);
        setDisplayName(displayName);
        setType(type);
        setIsTagService(EMBEDDED_SERVICEDEF_TAG_NAME.equals(type));
        setIsEnabled(isEnabled);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Boolean getIsTagService() {
        return isTagService;
    }

    public void setIsTagService(Boolean isTagService) {
        this.isTagService = isTagService;
    }
}
