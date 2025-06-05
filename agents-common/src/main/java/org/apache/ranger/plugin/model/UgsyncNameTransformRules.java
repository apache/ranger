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
 *=-0987654321`o6 bftware distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.plugin.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.ranger.plugin.util.RangerUserStoreUtil;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class UgsyncNameTransformRules extends RangerBaseModelObject implements java.io.Serializable {

    private static final long   serialVersionUID = 1L;
    Map<String, String> nameTransformRules;
    private final String name;

    public UgsyncNameTransformRules() {this(null, null, null);}

    public UgsyncNameTransformRules(String guid, String name, Map<String, String> nameTransformRules) {
        super();
        setGuid(guid);
        setId(Long.valueOf(1));
        setVersion(Long.valueOf(1));
        this.name               = name;
        this.nameTransformRules = nameTransformRules;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getNameTransformRules() {
        return nameTransformRules;
    }

    @Override
    public String toString() {
        return "{nameTransformRules=" + RangerUserStoreUtil.getPrintableOptions(nameTransformRules)
                + "}";
    }
}