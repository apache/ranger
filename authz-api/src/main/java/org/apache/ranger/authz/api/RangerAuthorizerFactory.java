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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.AUTHORIZER_CREATION_FAILED;

public class RangerAuthorizerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizerFactory.class);

    public static final String PROPERTY_RANGER_AUTHORIZER_IMPL_CLASS = "ranger.authorizer.impl.class";
    public static final String DEFAULT_RANGER_AUTHORIZER_IMPL_CLASS  = "org.apache.ranger.authz.embedded.RangerEmbeddedAuthorizer";

    public static RangerAuthorizer createAuthorizer(Properties properties) throws RangerAuthzException {
        String implClass = properties != null ? properties.getProperty(PROPERTY_RANGER_AUTHORIZER_IMPL_CLASS, DEFAULT_RANGER_AUTHORIZER_IMPL_CLASS) : DEFAULT_RANGER_AUTHORIZER_IMPL_CLASS;

        try {
            LOG.info("creating authorizer implementation of type: {}", implClass);

            return (RangerAuthorizer) Class.forName(implClass).getDeclaredConstructor(Properties.class).newInstance(properties);
        } catch (Exception e) {
            throw new RangerAuthzException(AUTHORIZER_CREATION_FAILED, e, implClass);
        }
    }

    private RangerAuthorizerFactory() {
        // prevent instantiation
    }
}
