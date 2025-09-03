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

import java.util.Properties;

import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.AUTHZ_FACTORY_INITIALIZATION_FAILED;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.AUTHZ_FACTORY_NOT_INITIALIZED;

public class RangerAuthorizerFactory {
    public static final String PROPERTY_RANGER_AUTHORIZER_IMPL_CLASS = "ranger.authorizer.impl.class";
    public static final String DEFAULT_RANGER_AUTHORIZER_IMPL_CLASS  = "org.apache.ranger.authz.embedded.RangerEmbeddedAuthorizer";

    private static RangerAuthorizerFactory instance;

    private final Properties       properties;
    private final RangerAuthorizer authorizer;

    public static RangerAuthorizerFactory getOrCreateInstance(Properties properties) throws RangerAuthzException {
        RangerAuthorizerFactory instance = RangerAuthorizerFactory.instance;

        if (instance == null) {
            synchronized (RangerAuthorizerFactory.class) {
                instance = RangerAuthorizerFactory.instance;

                if (instance == null) {
                    instance = new RangerAuthorizerFactory(properties);

                    RangerAuthorizerFactory.instance = instance;
                }
            }
        }

        return instance;
    }

    public static RangerAuthorizerFactory getInstance() throws RangerAuthzException {
        RangerAuthorizerFactory ret = instance;

        if (ret == null) {
            throw new RangerAuthzException(AUTHZ_FACTORY_NOT_INITIALIZED);
        }

        return ret;
    }

    private RangerAuthorizerFactory(Properties properties) throws RangerAuthzException {
        this.properties = properties;

        String implClass = this.properties.getProperty(PROPERTY_RANGER_AUTHORIZER_IMPL_CLASS, DEFAULT_RANGER_AUTHORIZER_IMPL_CLASS);

        try {
            authorizer = (RangerAuthorizer) Class.forName(implClass).getDeclaredConstructor(Properties.class).newInstance(properties);
        } catch (Exception e) {
            throw new RangerAuthzException(AUTHZ_FACTORY_INITIALIZATION_FAILED, e);
        }
    }

    public RangerAuthorizer getAuthorizer() {
        return authorizer;
    }
}
