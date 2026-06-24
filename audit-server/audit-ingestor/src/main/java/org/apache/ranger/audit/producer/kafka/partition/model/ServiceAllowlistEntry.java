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

package org.apache.ranger.audit.producer.kafka.partition.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ServiceAllowlistEntry implements Serializable {
    private final List<String> allowedUsers;
    private final String source;
    private final String notes;
    private final String pluginId;

    @JsonCreator
    public ServiceAllowlistEntry(@JsonProperty("allowedUsers") List<String> allowedUsers, @JsonProperty("source") String source, @JsonProperty("notes") String notes, @JsonProperty("pluginId") String pluginId) {
        this.allowedUsers = copyAllowedUsers(allowedUsers);
        this.source       = source;
        this.notes        = notes;
        this.pluginId     = pluginId;
    }

    public static ServiceAllowlistEntry ofUsers(String... users) {
        return new ServiceAllowlistEntry(List.of(users), null, null, null);
    }

    public static ServiceAllowlistEntry ofUsers(List<String> users) {
        return new ServiceAllowlistEntry(users, null, null, null);
    }

    public static ServiceAllowlistEntry ofUsers(List<String> users, String pluginId) {
        return new ServiceAllowlistEntry(users, null, null, pluginId);
    }

    private static List<String> copyAllowedUsers(List<String> allowedUsers) {
        if (allowedUsers == null || allowedUsers.isEmpty()) {
            return Collections.emptyList();
        }
        Set<String> unique = new LinkedHashSet<>();
        for (String user : allowedUsers) {
            if (user != null && !user.isBlank()) {
                unique.add(user.trim());
            }
        }
        return List.copyOf(unique);
    }

    public List<String> getAllowedUsers() {
        return allowedUsers;
    }

    public String getSource() {
        return source;
    }

    public String getNotes() {
        return notes;
    }

    public String getPluginId() {
        return pluginId;
    }

    /** True when normalized allowedUsers match (ignores source, notes, and pluginId). */
    public boolean hasSameAllowedUsers(List<String> users) {
        return Objects.equals(allowedUsers, ofUsers(users).getAllowedUsers());
    }

    @Override
    public boolean equals(Object otherServiceAllowlistEntryObj) {
        if (this == otherServiceAllowlistEntryObj) {
            return true;
        }
        if (otherServiceAllowlistEntryObj == null || getClass() != otherServiceAllowlistEntryObj.getClass()) {
            return false;
        }
        ServiceAllowlistEntry otherAllowlistEntry = (ServiceAllowlistEntry) otherServiceAllowlistEntryObj;
        return Objects.equals(allowedUsers, otherAllowlistEntry.allowedUsers)
                && Objects.equals(source, otherAllowlistEntry.source)
                && Objects.equals(notes, otherAllowlistEntry.notes)
                && Objects.equals(pluginId, otherAllowlistEntry.pluginId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(allowedUsers, source, notes, pluginId);
    }
}
