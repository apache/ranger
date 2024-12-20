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

import org.apache.hadoop.conf.Configuration;

// TODO remove this in favor of Guice DI
public class HbaseFactory {
    static final HbaseUserUtils userUtils = new HbaseUserUtilsImpl();
    static final HbaseAuthUtils authUtils = new HbaseAuthUtilsImpl();
    static final HbaseFactory   factory   = new HbaseFactory();

    /**
     * This is a singleton
     */
    private HbaseFactory() {
        // TODO remove this clutch to enforce singleton by moving to a DI framework
    }

    static HbaseFactory getInstance() {
        return factory;
    }

    static void initialize(Configuration conf) {
        HbaseUserUtilsImpl.initialize(conf);
    }

    HbaseAuthUtils getAuthUtils() {
        return authUtils;
    }

    HbaseUserUtils getUserUtils() {
        return userUtils;
    }

    HbaseAuditHandler getAuditHandler() {
        return new HbaseAuditHandlerImpl();
    }
}
