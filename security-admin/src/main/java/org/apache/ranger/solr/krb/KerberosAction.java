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

package org.apache.ranger.solr.krb;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;

import javax.security.auth.login.LoginException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Helper class for processors to perform an action as a KerberosUser.
 */
public class KerberosAction<T> {

    private final KerberosUser kerberosUser;
    private final PrivilegedExceptionAction<T> action;
    private final Logger logger;

    public KerberosAction(final KerberosUser kerberosUser,
                          final PrivilegedExceptionAction<T> action,
                          final Logger logger) {
        this.kerberosUser = kerberosUser;
        this.action = action;
        this.logger = logger;
        Validate.notNull(this.kerberosUser);
        Validate.notNull(this.action);
        Validate.notNull(this.logger);
    }

    public T execute() throws Exception {
        T result;
        // lazily login the first time the processor executes
        if (!kerberosUser.isLoggedIn()) {
            try {
                kerberosUser.login();
                if (logger != null) logger.info("Successful login for " + kerberosUser.getPrincipal());
            } catch (LoginException e) {
                throw new Exception("Login failed due to: " + e.getMessage(), e);
            }
        }

        // check if we need to re-login, will only happen if re-login window is reached (80% of TGT life)
        try {
            kerberosUser.checkTGTAndRelogin();
        } catch (LoginException e) {
            throw new Exception("Relogin check failed due to: " + e.getMessage(), e);
        }

        // attempt to execute the action, if an exception is caught attempt to logout/login and retry
        try {
            result = kerberosUser.doAs(action);
        } catch (SecurityException se) {
            if (logger != null) {
                logger.info("Privileged action failed, attempting relogin and retrying...");
                logger.debug("", se);
            }

            try {
                kerberosUser.logout();
                kerberosUser.login();
                result = kerberosUser.doAs(action);
            } catch (Exception e) {
                throw new Exception("Retrying privileged action failed due to: " + e.getMessage(), e);
            }
        } catch (PrivilegedActionException pae) {
            final Exception cause = pae.getException();
            throw new Exception("Privileged action failed due to: " + cause.getMessage(), cause);
        }

        return result;
    }
}
