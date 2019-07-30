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

package org.apache.ranger.authorization.ozone.authorizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneAclException;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;

public class RangerOzoneAuthorizer implements IAccessAuthorizer {

    private static final Log LOG  = LogFactory.getLog(RangerOzoneAuthorizer.class);

    private static final String   RANGER_PLUGIN_TYPE                       = "ozone";
    private static final String   RANGER_OZONE_AUTHORIZER_IMPL_CLASSNAME   = "org.apache.ranger.authorization.ozone.authorizer.RangerOzoneAuthorizer";
    private static RangerPluginClassLoader rangerPluginClassLoader         = null;
    private IAccessAuthorizer ozoneAuthorizationProviderImpl               = null;

    public RangerOzoneAuthorizer() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerOzoneAuthorizer.RangerOzoneAuthorizer()");
        }

        this.init();

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerOzoneAuthorizer.RangerOzoneAuthorizer()");
        }
    }

    private void init(){
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerOzoneAuthorizer.init()");
        }

        try {

            rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

            @SuppressWarnings("unchecked")
            Class<IAccessAuthorizer> cls = (Class<IAccessAuthorizer>) Class.forName(RANGER_OZONE_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);

            activatePluginClassLoader();

            ozoneAuthorizationProviderImpl = cls.newInstance();
        } catch (Exception e) {
            // check what need to be done
            LOG.error("Error Enabling RangerOzonePlugin", e);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerOzoneAuthorizer.init()");
        }
    }

    @Override
    public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context) throws OzoneAclException {

        boolean ret = false;

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerOzoneAuthorizer.checkAccess()");
        }

        try {
            activatePluginClassLoader();

            ret = ozoneAuthorizationProviderImpl.checkAccess(ozoneObject, context);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerOzoneAuthorizer.checkAccess()");
        }

        return ret;
    }

    private void activatePluginClassLoader() {
        if(rangerPluginClassLoader != null) {
            rangerPluginClassLoader.activate();
        }
    }

    private void deactivatePluginClassLoader() {
        if(rangerPluginClassLoader != null) {
            rangerPluginClassLoader.deactivate();
        }
    }
}
