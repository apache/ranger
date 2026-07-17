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

package org.apache.ranger.entraid.graph;

import org.apache.ranger.ugsyncutil.model.graph.DeltaPage;
import org.apache.ranger.ugsyncutil.model.graph.GraphGroup;
import org.apache.ranger.ugsyncutil.model.graph.GraphMemberRef;
import org.apache.ranger.ugsyncutil.model.graph.GraphUser;
import org.apache.ranger.ugsyncutil.model.graph.GroupMembershipPage;
import org.apache.ranger.ugsyncutil.model.graph.MembershipMode;

import java.io.Closeable;
import java.util.List;

public interface EntraIdGraphClient extends Closeable {
    void init(EntraIdGraphConfig config) throws GraphClientException;

    DeltaPage<GraphUser> getUserDelta(String deltaLink) throws GraphClientException;

    DeltaPage<GraphGroup> getGroupDelta(String deltaLink) throws GraphClientException;

    GroupMembershipPage getGroupDeltaWithMembers(String deltaLink) throws GraphClientException;

    List<GraphMemberRef> getGroupMembers(String groupId, MembershipMode mode) throws GraphClientException;

    @Override
    void close() throws GraphClientException;
}
