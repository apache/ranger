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

package org.apache.ranger.authz.api;

import org.apache.ranger.authz.DummyAuthorizer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.ranger.authz.api.RangerAuthorizerFactory.DEFAULT_RANGER_AUTHORIZER_IMPL_CLASS;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.AUTHORIZER_CREATION_FAILED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public class TestAuthorizerFactory {
    @Test
    public void testFactoryNullProperties() {
        try {
            RangerAuthorizer ignored = RangerAuthorizerFactory.createAuthorizer(null);

            fail("RangerAuthorizerFactory.createAuthorizer() should have thrown exception");
        } catch (Exception excp) {
            assertEquals(AUTHORIZER_CREATION_FAILED.getFormattedMessage(DEFAULT_RANGER_AUTHORIZER_IMPL_CLASS), excp.getMessage());
        }
    }

    @Test
    public void testFactoryEmptyProperties() {
        try {
            RangerAuthorizer ignored = RangerAuthorizerFactory.createAuthorizer(new Properties());

            fail("RangerAuthorizerFactory.createAuthorizer() should have failed to create authorizer " + DEFAULT_RANGER_AUTHORIZER_IMPL_CLASS);
        } catch (Exception excp) {
            assertEquals(AUTHORIZER_CREATION_FAILED.getFormattedMessage(DEFAULT_RANGER_AUTHORIZER_IMPL_CLASS), excp.getMessage());
        }
    }

    @Test
    public void testInvalidAuthorizer() {
        Properties prop = new Properties();

        prop.put(RangerAuthorizerFactory.PROPERTY_RANGER_AUTHORIZER_IMPL_CLASS, "org.apache.ranger.authz.api.InvalidAuthorizer");

        try {
            RangerAuthorizer ignored = RangerAuthorizerFactory.createAuthorizer(prop);

            fail("RangerAuthorizerFactory.createAuthorizer() should have failed to create authorizer org.apache.ranger.authz.api.InvalidAuthorizer");
        } catch (Exception excp) {
            assertEquals(AUTHORIZER_CREATION_FAILED.getFormattedMessage("org.apache.ranger.authz.api.InvalidAuthorizer"), excp.getMessage());
        }
    }

    @Test
    public void testCustomAuthorizer() {
        Properties prop = new Properties();

        prop.put(RangerAuthorizerFactory.PROPERTY_RANGER_AUTHORIZER_IMPL_CLASS, DummyAuthorizer.class.getCanonicalName());

        try {
            RangerAuthorizer authorizer = RangerAuthorizerFactory.createAuthorizer(prop);

            assertNotNull(authorizer);
        } catch (Exception excp) {
            fail("RangerAuthorizerFactory.createAuthorizer() failed to create authorizer " + DummyAuthorizer.class.getCanonicalName(), excp);
        }
    }
}
