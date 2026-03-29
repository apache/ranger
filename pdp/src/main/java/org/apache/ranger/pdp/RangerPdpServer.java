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

package org.apache.ranger.pdp;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.valves.AccessLogValve;
import org.apache.coyote.http2.Http2Protocol;
import org.apache.ranger.authz.api.RangerAuthorizer;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.embedded.RangerEmbeddedAuthorizer;
import org.apache.ranger.pdp.config.RangerPdpConfig;
import org.apache.ranger.pdp.config.RangerPdpConstants;
import org.apache.ranger.pdp.rest.RangerPdpApplication;
import org.apache.ranger.pdp.security.RangerPdpAuthNFilter;
import org.apache.ranger.pdp.security.RangerPdpRequestContextFilter;
import org.apache.tomcat.util.descriptor.web.FilterDef;
import org.apache.tomcat.util.descriptor.web.FilterMap;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;

import java.io.File;

/**
 * Main entry point for the Ranger Policy Decision Point (PDP) server.
 *
 * <p>Starts an embedded Apache Tomcat instance that:
 * <ul>
 *   <li>Creates and initialises a {@link RangerEmbeddedAuthorizer} singleton
 *   <li>Exposes the three authorizer methods as REST endpoints under {@code /authz/v1/}
 *   <li>Enforces authentication via {@link RangerPdpAuthNFilter} (Kerberos/JWT/HTTP-Header)
 *   <li>Optionally enables HTTP/2 ({@code Http2Protocol} upgrade on the connector)
 * </ul>
 *
 * <p><b>Startup:</b> {@code java -jar ranger-pdp.jar}
 * <br>Override config with: {@code -Dranger.pdp.conf.dir=/etc/ranger/pdp}
 */
public class RangerPdpServer {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPdpServer.class);

    private final RangerPdpConfig  config;
    private final RangerPdpStats   runtimeStats = new RangerPdpStats();
    private       Tomcat           tomcat;
    private       RangerAuthorizer authorizer;

    public RangerPdpServer() {
        this.config = new RangerPdpConfig();
    }

    public static void main(String[] args) throws Exception {
        new RangerPdpServer().start();
    }

    public void start() throws Exception {
        LOG.info("Starting Ranger PDP server");

        initAuthorizer();
        startTomcat();
    }

    public void stop() {
        LOG.info("Stopping Ranger PDP server");

        runtimeStats.setAcceptingRequests(false);
        runtimeStats.setServerStarted(false);

        if (tomcat != null) {
            try {
                if (tomcat.getConnector() != null) {
                    tomcat.getConnector().pause();
                }

                tomcat.stop();
                tomcat.destroy();
            } catch (LifecycleException e) {
                LOG.warn("Error stopping Tomcat", e);
            }
        }

        if (authorizer != null) {
            try {
                authorizer.close();
            } catch (RangerAuthzException e) {
                LOG.warn("Error closing authorizer", e);
            }
        }
    }

    private void initAuthorizer() throws RangerAuthzException {
        RangerAuthorizer authorizer = new RangerEmbeddedAuthorizer(config.getAuthzProperties());

        authorizer.init();

        this.authorizer = authorizer;

        runtimeStats.setAuthorizerInitialized(true);

        LOG.info("RangerEmbeddedAuthorizer initialised");
    }

    private void startTomcat() throws Exception {
        tomcat = new Tomcat();

        tomcat.setConnector(createConnector());

        String docBase = new File(System.getProperty("java.io.tmpdir"), "ranger-pdp-webapps").getAbsolutePath();

        new File(docBase).mkdirs();

        Context ctx = tomcat.addContext("", docBase);

        ctx.getServletContext().setAttribute(RangerPdpConstants.SERVLET_CTX_ATTR_AUTHORIZER, authorizer);
        ctx.getServletContext().setAttribute(RangerPdpConstants.SERVLET_CTX_ATTR_CONFIG, config);
        ctx.getServletContext().setAttribute(RangerPdpConstants.SERVLET_CTX_ATTR_RUNTIME_STATE, runtimeStats);

        addAuthFilter(ctx);
        addJerseyServlet(ctx);
        addStatusEndpoints(ctx);
        addAccessLogValve();

        Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "ranger-pdp-shutdown"));

        tomcat.start();

        runtimeStats.setServerStarted(true);
        runtimeStats.setAcceptingRequests(true);

        LOG.info("Ranger PDP server listening on port {} (SSL={}, HTTP/2={})", config.getPort(), config.isSslEnabled(), config.isHttp2Enabled());

        tomcat.getServer().await();
    }

    /**
     * Builds the Tomcat connector.
     *
     * <p>When SSL is enabled the connector is configured as an HTTPS endpoint.
     * When HTTP/2 is enabled an {@link Http2Protocol} upgrade protocol is added,
     * supporting both {@code h2} (over TLS) and {@code h2c} (cleartext upgrade) depending
     * on whether SSL is enabled.
     */
    private Connector createConnector() {
        Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");

        connector.setPort(config.getPort());
        connector.setProperty("maxThreads", String.valueOf(config.getHttpConnectorMaxThreads()));
        connector.setProperty("minSpareThreads", String.valueOf(config.getHttpConnectorMinSpareThreads()));
        connector.setProperty("acceptCount", String.valueOf(config.getHttpConnectorAcceptCount()));
        connector.setProperty("maxConnections", String.valueOf(config.getHttpConnectorMaxConnections()));

        LOG.info("Configured HTTP connector limits: maxThreads={}, minSpareThreads={}, acceptCount={}, maxConnections={}",
                config.getHttpConnectorMaxThreads(), config.getHttpConnectorMinSpareThreads(),
                config.getHttpConnectorAcceptCount(), config.getHttpConnectorMaxConnections());

        if (config.isSslEnabled()) {
            connector.setSecure(true);
            connector.setScheme("https");
            connector.setProperty("SSLEnabled", "true");
            connector.setProperty("protocol", "org.apache.coyote.http11.Http11NioProtocol");
            connector.setProperty("keystoreFile", config.getKeystoreFile());
            connector.setProperty("keystorePass", config.getKeystorePassword());
            connector.setProperty("keystoreType", config.getKeystoreType());
            connector.setProperty("sslProtocol", "TLS");

            if (config.isTruststoreEnabled()) {
                connector.setProperty("truststoreFile",  config.getTruststoreFile());
                connector.setProperty("truststorePass",  config.getTruststorePassword());
                connector.setProperty("truststoreType",  config.getTruststoreType());
                connector.setProperty("clientAuth",      "true");
            }
        }

        if (config.isHttp2Enabled()) {
            connector.addUpgradeProtocol(new Http2Protocol());

            LOG.info("HTTP/2 upgrade protocol registered on connector (port={})", config.getPort());
        }

        return connector;
    }

    /**
     * Registers {@link RangerPdpAuthNFilter} on all {@code /authz/*} paths.
     * Init parameters are forwarded from the server config so the filter can
     * instantiate and configure the auth handlers.
     */
    private void addAuthFilter(Context ctx) {
        FilterDef reqCtxFilterDef = new FilterDef();
        FilterMap reqCtxFilterMap = new FilterMap();
        FilterDef authFilterDef = new FilterDef();
        FilterMap authFilterMap = new FilterMap();

        reqCtxFilterDef.setFilterName("rangerPdpRequestContextFilter");
        reqCtxFilterDef.setFilter(new RangerPdpRequestContextFilter());

        reqCtxFilterMap.setFilterName("rangerPdpRequestContextFilter");
        reqCtxFilterMap.addURLPattern("/*");

        authFilterDef.setFilterName("rangerPdpAuthFilter");
        authFilterDef.setFilter(new RangerPdpAuthNFilter());
        authFilterDef.addInitParameter(RangerPdpConstants.PROP_AUTHN_TYPES, config.getAuthnTypes());

        // HTTP Header auth
        authFilterDef.addInitParameter(RangerPdpConstants.PROP_AUTHN_HEADER_ENABLED, Boolean.toString(config.isHeaderAuthnEnabled()));
        authFilterDef.addInitParameter(RangerPdpConstants.PROP_AUTHN_HEADER_USERNAME, config.getHeaderAuthnUsername());

        // JWT bearer token auth
        authFilterDef.addInitParameter(RangerPdpConstants.PROP_AUTHN_JWT_ENABLED, Boolean.toString(config.isJwtAuthnEnabled()));
        authFilterDef.addInitParameter(RangerPdpConstants.PROP_AUTHN_JWT_PROVIDER_URL, config.getJwtProviderUrl());
        authFilterDef.addInitParameter(RangerPdpConstants.PROP_AUTHN_JWT_PUBLIC_KEY, config.getJwtPublicKey());
        authFilterDef.addInitParameter(RangerPdpConstants.PROP_AUTHN_JWT_COOKIE_NAME, config.getJwtCookieName());
        authFilterDef.addInitParameter(RangerPdpConstants.PROP_AUTHN_JWT_AUDIENCES, config.getJwtAudiences());

        // Kerberos / SPNEGO
        authFilterDef.addInitParameter(RangerPdpConstants.PROP_AUTHN_KERBEROS_ENABLED, Boolean.toString(config.isKerberosAuthnEnabled()));
        authFilterDef.addInitParameter(RangerPdpConstants.PROP_AUTHN_KERBEROS_SPNEGO_PRINCIPAL,   config.getSpnegoPrincipal());
        authFilterDef.addInitParameter(RangerPdpConstants.PROP_AUTHN_KERBEROS_SPNEGO_KEYTAB,      config.getSpnegoKeytab());
        authFilterDef.addInitParameter(RangerPdpConstants.PROP_AUTHN_KERBEROS_NAME_RULES,         config.getKerberosNameRules());
        authFilterDef.addInitParameter(RangerPdpConstants.PROP_AUTHN_KERBEROS_KRB_TOKEN_VALIDITY, String.valueOf(config.getKerberosTokenValiditySeconds()));

        authFilterMap.setFilterName("rangerPdpAuthFilter");
        authFilterMap.addURLPattern("/authz/*");

        ctx.addFilterDef(reqCtxFilterDef);
        ctx.addFilterMap(reqCtxFilterMap);
        ctx.addFilterDef(authFilterDef);
        ctx.addFilterMap(authFilterMap);
    }

    /**
     * Registers the Jersey {@link ServletContainer} backed by {@link RangerPdpApplication}.
     * The Jersey application binds the {@link RangerAuthorizer} singleton via HK2 so
     * that it can be injected into {@link org.apache.ranger.pdp.rest.RangerPdpREST}.
     */
    private void addJerseyServlet(Context ctx) {
        RangerPdpApplication jerseyApp     = new RangerPdpApplication(authorizer, config);
        Servlet              jerseyServlet = new ServletContainer(jerseyApp);

        Tomcat.addServlet(ctx, "jerseyServlet", jerseyServlet).setLoadOnStartup(1);

        ctx.addServletMappingDecoded("/authz/*", "jerseyServlet");
    }

    private void addStatusEndpoints(Context ctx) {
        HttpServlet liveServlet    = new RangerPdpStatusServlet(runtimeStats, config, RangerPdpStatusServlet.Mode.LIVE);
        HttpServlet readyServlet   = new RangerPdpStatusServlet(runtimeStats, config, RangerPdpStatusServlet.Mode.READY);
        HttpServlet metricsServlet = new RangerPdpStatusServlet(runtimeStats, config, RangerPdpStatusServlet.Mode.METRICS);

        Tomcat.addServlet(ctx, "pdpLiveServlet", liveServlet).setLoadOnStartup(1);
        Tomcat.addServlet(ctx, "pdpReadyServlet", readyServlet).setLoadOnStartup(1);
        Tomcat.addServlet(ctx, "pdpMetricsServlet", metricsServlet).setLoadOnStartup(1);

        ctx.addServletMappingDecoded("/health/live", "pdpLiveServlet");
        ctx.addServletMappingDecoded("/health/ready", "pdpReadyServlet");
        ctx.addServletMappingDecoded("/metrics", "pdpMetricsServlet");
    }

    private void addAccessLogValve() {
        String logDir = config.getLogDir();

        new File(logDir).mkdirs();

        AccessLogValve valve = new AccessLogValve();

        valve.setDirectory(logDir);
        valve.setPrefix("ranger-pdp-access");
        valve.setSuffix(".log");
        valve.setPattern("%h %l %u %t \"%r\" %s %b %D");
        valve.setRotatable(true);

        tomcat.getHost().getPipeline().addValve(valve);
    }
}
