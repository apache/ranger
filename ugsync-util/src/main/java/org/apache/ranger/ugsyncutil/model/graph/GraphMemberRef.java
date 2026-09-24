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

import java.util.Objects;

public final class GraphMemberRef {
    private final String id;
    private final MemberType type;
    private final boolean removed;

    public GraphMemberRef(String id, MemberType type, boolean removed) {
        this.id = id;
        this.type = type == null ? MemberType.OTHER : type;
        this.removed = removed;
    }

    public static GraphMemberRef added(String id, MemberType type) {
        return new GraphMemberRef(id, type, false);
    }

    public static GraphMemberRef removed(String id, MemberType type) {
        return new GraphMemberRef(id, type, true);
    }

    public String getId() {
        return id;
    }

    public MemberType getType() {
        return type;
    }

    public boolean isRemoved() {
        return removed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GraphMemberRef that = (GraphMemberRef) o;
        return removed == that.removed && Objects.equals(id, that.id) && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, removed);
    }

    @Override
    public String toString() {
        return "GraphMemberRef{id=" + id + ", type=" + type + ", removed=" + removed + '}';
    }
}
