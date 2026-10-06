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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class GraphGroup {
    private String id;
    private String displayName;
    private String mailNickname;
    private boolean securityEnabled = true;
    private final Map<String, String> additionalAttributes = new LinkedHashMap<>();
    private final List<GraphMemberRef> memberChanges = new ArrayList<>();

    public GraphGroup() {
    }

    public GraphGroup(String id, String displayName) {
        this.id = id;
        this.displayName = displayName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getMailNickname() {
        return mailNickname;
    }

    public void setMailNickname(String mailNickname) {
        this.mailNickname = mailNickname;
    }

    public boolean isSecurityEnabled() {
        return securityEnabled;
    }

    public void setSecurityEnabled(boolean securityEnabled) {
        this.securityEnabled = securityEnabled;
    }

    public Map<String, String> getAdditionalAttributes() {
        return Collections.unmodifiableMap(additionalAttributes);
    }

    public void putAdditionalAttribute(String key, String value) {
        if (key != null) {
            additionalAttributes.put(key, value);
        }
    }

    public List<GraphMemberRef> getMemberChanges() {
        return Collections.unmodifiableList(memberChanges);
    }

    public void addMemberChange(GraphMemberRef ref) {
        if (ref != null) {
            memberChanges.add(ref);
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
        GraphGroup that = (GraphGroup) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        return "GraphGroup{"
                + "id=" + id
                + ", displayName=" + displayName
                + ", mailNickname=" + mailNickname
                + ", securityEnabled=" + securityEnabled
                + ", additionalAttributes=" + additionalAttributes
                + ", memberChanges=" + memberChanges.size()
                + '}';
    }
}
