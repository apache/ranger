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
package org.apache.ranger.authorization.hbase;

import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.access.Permission;

import java.io.IOException;

public abstract class CompatMasterObserver implements MasterObserver {
    @Override
    public void preBalance(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        preBalanceHookAction(ctx, "balance", Permission.Action.ADMIN, null);
    }

    abstract public void preBalanceHookAction(ObserverContext<MasterCoprocessorEnvironment> ctx, String request,
                                              Permission.Action action, Object balanceRequest) throws AccessDeniedException;
}
