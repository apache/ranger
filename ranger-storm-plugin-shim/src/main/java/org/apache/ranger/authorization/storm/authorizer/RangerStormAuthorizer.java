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

package org.apache.ranger.authorization.storm.authorizer;

import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RangerStormAuthorizer implements IAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(RangerStormAuthorizer.class);

    private static final String RANGER_PLUGIN_TYPE                     = "storm";
    private static final String RANGER_STORM_AUTHORIZER_IMPL_CLASSNAME = "org.apache.ranger.authorization.storm.authorizer.RangerStormAuthorizer";

    private IAuthorizer             rangerStormAuthorizerImpl;
    private RangerPluginClassLoader rangerPluginClassLoader;

    public RangerStormAuthorizer() {
        LOG.debug("==> RangerStormAuthorizer.RangerStormAuthorizer()");

        this.init();

        LOG.debug("<== RangerStormAuthorizer.RangerStormAuthorizer()");
    }

    @Override
    public void prepare(Map stormConf) {
        LOG.debug("==> RangerStormAuthorizer.prepare()");

        try {
            activatePluginClassLoader();

            rangerStormAuthorizerImpl.prepare(stormConf);
        } finally {
            deactivatePluginClassLoader();
        }

        LOG.debug("<== RangerStormAuthorizer.prepare()");
    }

    @Override
    public boolean permit(ReqContext context, String operation, Map topologyConf) {
        LOG.debug("==> RangerStormAuthorizer.permit()");

        try {
            activatePluginClassLoader();

            return rangerStormAuthorizerImpl.permit(context, operation, topologyConf);
        } finally {
            deactivatePluginClassLoader();

            LOG.debug("<== RangerStormAuthorizer.permit()");
        }
    }

    private void init() {
        LOG.debug("==> RangerStormAuthorizer.init()");

        try {
            rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

            @SuppressWarnings("unchecked")
            Class<IAuthorizer> cls = (Class<IAuthorizer>) Class.forName(RANGER_STORM_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);

            activatePluginClassLoader();

            rangerStormAuthorizerImpl = cls.newInstance();
        } catch (Exception e) {
            // check what need to be done
            LOG.error("Error Enabling RangerStormPlugin", e);
        } finally {
            deactivatePluginClassLoader();
        }

        LOG.debug("<== RangerStormAuthorizer.init()");
    }

    private void activatePluginClassLoader() {
        if (rangerPluginClassLoader != null) {
            rangerPluginClassLoader.activate();
        }
    }

    private void deactivatePluginClassLoader() {
        if (rangerPluginClassLoader != null) {
            rangerPluginClassLoader.deactivate();
        }
    }
}
