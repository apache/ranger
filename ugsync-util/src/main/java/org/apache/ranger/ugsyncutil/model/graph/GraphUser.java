/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.ugsyncutil.model.graph;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class GraphUser {
    private String id;
    private String userPrincipalName;
    private String mail;
    private String displayName;
    private boolean accountEnabled = true;

    private final Map<String, String> additionalAttributes = new LinkedHashMap<>();

    public GraphUser() {
    }

    public GraphUser(String id, String userPrincipalName, String mail, String displayName) {
        this.id = id;
        this.userPrincipalName = userPrincipalName;
        this.mail = mail;
        this.displayName = displayName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUserPrincipalName() {
        return userPrincipalName;
    }

    public void setUserPrincipalName(String userPrincipalName) {
        this.userPrincipalName = userPrincipalName;
    }

    public String getMail() {
        return mail;
    }

    public void setMail(String mail) {
        this.mail = mail;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public boolean isAccountEnabled() {
        return accountEnabled;
    }

    public void setAccountEnabled(boolean accountEnabled) {
        this.accountEnabled = accountEnabled;
    }

    public Map<String, String> getAdditionalAttributes() {
        return Collections.unmodifiableMap(additionalAttributes);
    }

    public void putAdditionalAttribute(String key, String value) {
        if (key != null) {
            additionalAttributes.put(key, value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GraphUser that = (GraphUser) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        return "GraphUser{id=" + id
                + ", userPrincipalName=" + userPrincipalName
                + ", mail=" + mail
                + ", displayName=" + displayName
                + ", accountEnabled=" + accountEnabled
                + ", additionalAttributes=" + additionalAttributes
                + '}';
    }
}
