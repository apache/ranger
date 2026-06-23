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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractKerberosUser implements KerberosUser {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractKerberosUser.class);

    static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    /**
     * Percentage of the ticket window to use before we renew the TGT.
     */
    static final float TICKET_RENEW_WINDOW = 0.80f;

    protected final AtomicBoolean loggedIn = new AtomicBoolean(false);

    protected Subject      subject;
    protected LoginContext loginContext;

    public AbstractKerberosUser() {
    }

    /**
     * Performs a login using the specified principal and keytab.
     *
     * @throws LoginException if the login fails
     */
    @Override
    public synchronized void login() throws LoginException {
        if (isLoggedIn()) {
            return;
        }

        try {
            // If it's the first time ever calling login then we need to initialize a new context
            if (loginContext == null) {
                LOG.debug("Initializing new login context...");

                if (this.subject == null) {
                    // only create a new subject if a current one does not exist
                    // other classes may be referencing an existing subject and replacing it may break functionality of those other classes after relogin
                    this.subject = new Subject();
                }

                this.loginContext = createLoginContext(subject);
            }

            loginContext.login();
            loggedIn.set(true);

            LOG.info("Successful login for {}", getPrincipal());
        } catch (LoginException le) {
            LoginException loginException = new LoginException("Unable to login with " + getPrincipal() + " due to: " + le.getMessage());

            loginException.setStackTrace(le.getStackTrace());

            throw loginException;
        }
    }

    /**
     * Performs a logout of the current user.
     *
     * @throws LoginException if the logout fails
     */
    @Override
    public synchronized void logout() throws LoginException {
        if (!isLoggedIn()) {
            return;
        }

        try {
            loginContext.logout();
            loggedIn.set(false);

            LOG.info("Successful logout for {}", getPrincipal());

            loginContext = null;
        } catch (LoginException e) {
            LOG.warn("Logout failed due to: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Executes the PrivilegedAction as this user.
     *
     * @param action the action to execute
     * @param <T> the type of result
     * @return the result of the action
     * @throws IllegalStateException if this method is called while not logged in
     */
    @Override
    public <T> T doAs(final PrivilegedAction<T> action) throws IllegalStateException {
        if (!isLoggedIn()) {
            throw new IllegalStateException("Must login before executing actions");
        }

        return Subject.doAs(subject, action);
    }

    /**
     * Executes the PrivilegedAction as this user.
     *
     * @param action the action to execute
     * @param <T> the type of result
     * @return the result of the action
     * @throws IllegalStateException if this method is called while not logged in
     * @throws PrivilegedActionException if an exception is thrown from the action
     */
    @Override
    public <T> T doAs(final PrivilegedExceptionAction<T> action) throws IllegalStateException, PrivilegedActionException {
        if (!isLoggedIn()) {
            throw new IllegalStateException("Must login before executing actions");
        }

        return Subject.doAs(subject, action);
    }

    /**
     * Proactively renew credentials when the TGT is missing or has passed
     * {@link #TICKET_RENEW_WINDOW} (80%) of its lifetime.
     *
     * <p>Called on every {@link KerberosAction#execute()} before the privileged action runs,
     * so long-lived Solr audit queries refresh before the TGT expires.
     *
     * @return {@code true} if relogin was performed, {@code false} if the TGT is still valid
     * @throws LoginException if relogin fails
     */
    @Override
    public synchronized boolean checkTGTAndRelogin() throws LoginException {
        final KerberosTicket tgt = getTGT();

        if (tgt == null) {
            LOG.debug("TGT was not found");
        }

        if (tgt != null && System.currentTimeMillis() < getRefreshTime(tgt)) {
            LOG.debug("TGT was found, but has not reached expiration window");

            return false;
        }

        LOG.info("Performing relogin for {}", getPrincipal());

        performRelogin();

        return true;
    }

    /**
     * Renews JAAS credentials for proactive TGT refresh and for error recovery.
     *
     * <p>With {@code useKeyTab=true} and {@code useTicketCache=true}
     * (shipped Solr dispatcher default), the previous {@code logout(); login()} sequence at
     * the 80% TGT renewal window leaves {@code Krb5LoginModule} with no encryption key for the
     * ticket cache and fails with {@code "No key to store"}, stopping audit indexing until
     * process restart. For keytab-backed users, calling {@code loginContext.login()} on the
     * existing {@link LoginContext} and {@link Subject} renews from the keytab without
     * clearing credentials first. The {@link Subject} is preserved across relogin because
     * other components may hold references to it.
     *
     * <p>Non-keytab principals, or callers that are not yet logged in, use the traditional
     * {@code logout(); login()} path. In-place keytab relogin failures also fall back to
     * that path.
     *
     * @throws LoginException if relogin fails
     * @see KerberosJAASConfigUser#useKeytabRelogin()
     */
    public synchronized void performRelogin() throws LoginException {
        if (useKeytabRelogin() && isLoggedIn() && loginContext != null) {
            try {
                // Renew TGT from keytab without logout(); avoids "No key to store" when useTicketCache=true.
                loginContext.login();
                loggedIn.set(true);

                LOG.info("Successful in-place keytab relogin for {}", getPrincipal());

                return;
            } catch (LoginException e) {
                LOG.warn("In-place keytab relogin failed for {}, falling back to logout/login: {}", getPrincipal(), e.getMessage());
            }
        }

        logout();
        login();
    }

    /**
     * Whether {@link #performRelogin()} should renew from keytab in place (no {@code logout()}
     * first). Default {@code false}; {@link KerberosJAASConfigUser} returns {@code true} when
     * JAAS {@code useKeyTab} is enabled. Gated on keytab capability, not on
     * {@code useTicketCache}, because only keytab principals can safely skip logout.
     *
     * @return {@code true} to use in-place keytab relogin
     */
    protected boolean useKeytabRelogin() {
        return false;
    }

    /**
     * @return true if this user is currently logged in, false otherwise
     */
    @Override
    public boolean isLoggedIn() {
        return loggedIn.get();
    }

    @Override
    public String toString() {
        return "KerberosUser{" + "principal='" + getPrincipal() + '\'' + ", loggedIn=" + loggedIn + '}';
    }

    protected abstract LoginContext createLoginContext(Subject subject) throws LoginException;

    /**
     * Returns the Kerberos TGT from the JAAS {@link Subject}, if present.
     *
     * @return the user's TGT, or {@code null} if the subject is uninitialized or has no TGT
     */
    private synchronized KerberosTicket getTGT() {
        // Subject is created lazily on first login(); null before that is expected.
        if (subject == null) {
            return null;
        }

        final Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);

        for (KerberosTicket ticket : tickets) {
            if (isTGSPrincipal(ticket.getServer())) {
                return ticket;
            }
        }

        return null;
    }

    /**
     * TGS must have the server principal of the form "krbtgt/FOO@FOO".
     *
     * @param principal the principal to check
     * @return true if the principal is the TGS, false otherwise
     */
    private boolean isTGSPrincipal(final KerberosPrincipal principal) {
        if (principal == null) {
            return false;
        }

        if (principal.getName().equals("krbtgt/" + principal.getRealm() + "@" + principal.getRealm())) {
            LOG.trace("Found TGT principal: {}", principal.getName());

            return true;
        }

        return false;
    }

    private long getRefreshTime(final KerberosTicket tgt) {
        long start = tgt.getStartTime().getTime();
        long end   = tgt.getEndTime().getTime();

        if (LOG.isTraceEnabled()) {
            final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
            final String           startDate  = dateFormat.format(new Date(start));
            final String           endDate    = dateFormat.format(new Date(end));

            LOG.trace("TGT valid starting at: {}", startDate);
            LOG.trace("TGT expires at: {}", endDate);
        }

        return start + (long) ((end - start) * TICKET_RENEW_WINDOW);
    }
}
