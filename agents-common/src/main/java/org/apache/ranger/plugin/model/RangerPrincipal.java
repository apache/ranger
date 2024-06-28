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
import java.util.Objects;

@JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RangerPrincipal implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    public enum PrincipalType { USER, GROUP, ROLE }

    private PrincipalType type;
    private String        name;

    public RangerPrincipal() {
        this(null, null);
    }

    public RangerPrincipal(PrincipalType type, String name) {
        setType(type);
        setName(name);
    }

    public PrincipalType getType() {
        return type;
    }

    public void setType(PrincipalType type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name);
    }

    @Override
    public boolean equals(Object obj) {
        final boolean ret;

        if (this == obj) {
            ret = true;
        } else if (obj == null) {
            ret = false;
        } else if (getClass() != obj.getClass()) {
            ret = false;
        } else {
            RangerPrincipal other = (RangerPrincipal) obj;

            ret = Objects.equals(type, other.type) && Objects.equals(name, other.name);
        }

        return ret;
    }

    @Override
    public String toString() {
        return "{type=" + type + ", name=" + name + "}";
    }
}
