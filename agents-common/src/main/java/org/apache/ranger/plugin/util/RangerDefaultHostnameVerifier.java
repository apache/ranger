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

package org.apache.ranger.plugin.util;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

public class RangerDefaultHostnameVerifier implements HostnameVerifier {
    private static final Logger LOG = LoggerFactory.getLogger(RangerDefaultHostnameVerifier.class);
    // Thread-safe, stateless, and standard Apache implementation
    private static final HostnameVerifier DELEGATE = new DefaultHostnameVerifier();

    @Override
    public boolean verify(String hostname, SSLSession session) {
        if (hostname == null || session == null) {
            return false;
        }
        boolean verified = DELEGATE.verify(hostname, session);
        if (!verified && LOG.isWarnEnabled()) {
            LOG.warn("Hostname verification failed for [{}]: certificate identity does not match requested host", hostname);
        }
        return verified;
    }
}
