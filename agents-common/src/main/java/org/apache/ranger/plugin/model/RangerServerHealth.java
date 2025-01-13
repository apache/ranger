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
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@JsonInclude(Include.NON_EMPTY)
public class RangerServerHealth {
    private final RangerServerStatus  status;
    private final Map<String, Object> details;

    private RangerServerHealth(Builder builder) {
        this.status  = builder.status;
        this.details = Collections.unmodifiableMap(builder.details);
    }

    public static Builder unknown() {
        return status(RangerServerStatus.UNKNOWN);
    }

    public static Builder up() {
        return status(RangerServerStatus.UP);
    }

    public static Builder down() {
        return status(RangerServerStatus.DOWN);
    }

    public static Builder status(RangerServerStatus status) {
        return new Builder(status);
    }

    public RangerServerStatus getStatus() {
        return this.status;
    }

    public Map<String, Object> getDetails() {
        return this.details;
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, details);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof RangerServerHealth)) {
            return false;
        }

        RangerServerHealth health = (RangerServerHealth) o;

        return Objects.equals(status, health.status) && Objects.equals(details, health.details);
    }

    @Override
    public String toString() {
        return getStatus() + " " + getDetails();
    }

    public enum RangerServerStatus {
        UNKNOWN, INITIALIZING, INITIALIZATION_FAILURE, UP, DOWN
    }

    public static class Builder {
        private final Map<String, Object> details;
        private       RangerServerStatus  status;

        public Builder() {
            this.status  = RangerServerStatus.UNKNOWN;
            this.details = new LinkedHashMap<>();
        }

        public Builder(RangerServerStatus status) {
            this.status  = status;
            this.details = new LinkedHashMap<>();
        }

        public Builder(RangerServerStatus status, Map<String, ?> details) {
            this.status  = status;
            this.details = new LinkedHashMap<>(details);
        }

        public Builder withDetail(String key, Object value) {
            this.details.put(key, value);
            return this;
        }

        public RangerServerHealth build() {
            return new RangerServerHealth(this);
        }
    }
}
