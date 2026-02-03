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
package org.apache.ranger.authz.authority;

import java.util.Set;
import org.springframework.security.core.GrantedAuthority;

public final class JwtAuthority implements GrantedAuthority {
    private static final long serialVersionUID = 12323L;
    private final String role;
    private final Set<String> groups;

    public JwtAuthority(String role, Set<String> groups) {
        this.role = role;
        this.groups = groups;
    }

    public String getAuthority() {
        return this.role;
    }

    public Set<String> getGroups() { return this.groups; }

    public String toString() {
        return this.role;
    }
}