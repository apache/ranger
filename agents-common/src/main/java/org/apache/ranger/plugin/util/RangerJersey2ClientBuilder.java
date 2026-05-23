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

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

/**
 * Comprehensive Jersey client utility for Ranger components.
 *
 * <p>This utility provides MOXy-safe Jersey client creation with the following guarantees:
 * <ul>
 *   <li>Jackson JSON provider is explicitly registered with high priority</li>
 *   <li>Jersey auto-discovery is disabled to prevent MOXy interference</li>
 *   <li>SSL/TLS configuration support for secure communications</li>
 *   <li>Configurable connection and read timeouts</li>
 *   <li>Comprehensive logging and validation</li>
 * </ul>
 *
 * <p><strong>Usage Patterns:</strong>
 * <pre>
 * // Basic client
 * Client client = RangerJersey2ClientBuilder.createStandardClient();
 *
 * // Client with timeouts
 * Client client = RangerJersey2ClientBuilder.createClient(5000, 30000);
 *
 * // Secure client with SSL
 * Client client = RangerJersey2ClientBuilder.createSecureClient(sslContext, hostnameVerifier, 5000, 30000);
 *
 * // Drop-in replacements for unsafe patterns
 * Client client = RangerJersey2ClientBuilder.newClient();  // Instead of ClientBuilder.newClient()
 * Client client = RangerJersey2ClientBuilder.newBuilder().build();  // Instead of ClientBuilder.newBuilder().build()
 * </pre>
 *
 * @author Apache Ranger Team
 * @since Ranger 3.0
 */
public class RangerJersey2ClientBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(RangerJersey2ClientBuilder.class);

    // Configuration constants
    private static final String DISABLE_AUTO_DISCOVERY_PROPERTY      = "jersey.config.disableAutoDiscovery";
    private static final String PROVIDER_SCANNING_RECURSIVE_PROPERTY = "jersey.config.server.provider.scanning.recursive";
    private static final String JERSEY_JACKSON_FEATURE_CLASS             = "org.glassfish.jersey.jackson.JacksonFeature";
    private static final String[] JACKSON_JSON_PROVIDER_CLASSES          = {
            "org.glassfish.jersey.jackson.internal.DefaultJacksonJaxbJsonProvider",
            "com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider",
    };
    private static final String RANGER_PLUGIN_CLASSLOADER_CLASS        = "org.apache.ranger.plugin.classloader.RangerPluginClassLoader";
    private static final int    DEFAULT_CONNECT_TIMEOUT_MS           = 5000;
    private static final int    DEFAULT_READ_TIMEOUT_MS              = 30000;

    // Private constructor to prevent instantiation
    private RangerJersey2ClientBuilder() {
        // Utility class - no instances
    }

    public static ClientConfig newClientConfig() {
        ClassLoader jerseyCl = resolveJerseyClassLoader();

        try {
            Class<?> configClass = Class.forName("org.glassfish.jersey.client.ClientConfig", true, jerseyCl);

            return ClientConfig.class.cast(configClass.getDeclaredConstructor().newInstance());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to create ClientConfig using classloader " + jerseyCl, e);
        }
    }

    /**
     * Creates a standard Jersey client with MOXy prevention and default timeouts.
     *
     * @return A configured Jersey client safe from MOXy interference
     */
    public static Client createStandardClient() {
        return createClient(DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS);
    }

    /**
     * Creates a Jersey client with MOXy prevention and custom timeouts.
     *
     * @param connectTimeoutMs Connection timeout in milliseconds
     * @param readTimeoutMs Read timeout in milliseconds
     * @return A configured Jersey client safe from MOXy interference
     */
    public static Client createClient(int connectTimeoutMs, int readTimeoutMs) {
        return runWithJerseyClassLoader(() -> {
            LOG.debug("Creating standard Jersey client with timeouts: connect={}ms, read={}ms", connectTimeoutMs, readTimeoutMs);

            ClientConfig config = newClientConfig();
            applyAntiMoxyConfiguration(config);

            config.property(ClientProperties.CONNECT_TIMEOUT, connectTimeoutMs);
            config.property(ClientProperties.READ_TIMEOUT, readTimeoutMs);

            Client client = ClientBuilder.newClient(config);
            validateAntiMoxyConfiguration(config);

            LOG.debug("Successfully created standard Jersey client");
            return client;
        });
    }

    /**
     * Creates a secure Jersey client with SSL/TLS support and MOXy prevention.
     *
     * @param sslContext SSL context for secure connections (can be null for default)
     * @param hostnameVerifier Hostname verifier for SSL validation (can be null for default)
     * @param connectTimeoutMs Connection timeout in milliseconds
     * @param readTimeoutMs Read timeout in milliseconds
     * @return A configured secure Jersey client safe from MOXy interference
     */
    public static Client createSecureClient(SSLContext sslContext, HostnameVerifier hostnameVerifier, int connectTimeoutMs, int readTimeoutMs) {
        return runWithJerseyClassLoader(() -> {
            LOG.debug("Creating secure Jersey client with SSL - connect={}ms, read={}ms", connectTimeoutMs, readTimeoutMs);

            ClientConfig config = newClientConfig();
            applyAntiMoxyConfiguration(config);

            config.property(ClientProperties.CONNECT_TIMEOUT, connectTimeoutMs);
            config.property(ClientProperties.READ_TIMEOUT, readTimeoutMs);

            ClientBuilder builder = ClientBuilder.newBuilder().withConfig(config);

            if (sslContext != null) {
                builder.sslContext(sslContext);
                LOG.debug("Applied custom SSL context");
            }

            if (hostnameVerifier != null) {
                builder.hostnameVerifier(hostnameVerifier);
                LOG.debug("Applied custom hostname verifier");
            }

            Client client = builder.build();
            validateAntiMoxyConfiguration(config);

            LOG.debug("Successfully created secure Jersey client");
            return client;
        });
    }

    // ========== DROP-IN REPLACEMENTS for unsafe ClientBuilder patterns ==========

    /**
     * Drop-in replacement for {@code ClientBuilder.newClient()}.
     * Creates a MOXy-safe client with default configuration.
     *
     * @return A configured Jersey client safe from MOXy interference
     */
    public static Client newClient() {
        LOG.debug("Creating MOXy-safe client as drop-in replacement for ClientBuilder.newClient()");
        validateSafeUsage("newClient()");
        return createStandardClient();
    }

    /**
     * Drop-in replacement for {@code ClientBuilder.newBuilder()}.
     * Returns a builder that creates MOXy-safe clients.
     *
     * @return A safe client builder
     */
    public static SafeClientBuilder newBuilder() {
        LOG.debug("Creating MOXy-safe builder as drop-in replacement for ClientBuilder.newBuilder()");
        validateSafeUsage("newBuilder()");
        return new SafeClientBuilder();
    }

    /**
     * Safe client builder that ensures MOXy prevention in all created clients.
     * This class provides a familiar builder pattern while ensuring security.
     */
    public static class SafeClientBuilder {
        private SSLContext       sslContext;
        private HostnameVerifier hostnameVerifier;
        private int              connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;
        private int              readTimeoutMs    = DEFAULT_READ_TIMEOUT_MS;
        private ClientConfig     clientConfig;

        // Package-private constructor
        SafeClientBuilder() {
            // Only accessible through RangerJersey2ClientBuilder.newBuilder()
        }

        /**
         * Configures SSL context for the client.
         *
         * @param sslContext SSL context to use
         * @return This builder for method chaining
         */
        public SafeClientBuilder sslContext(SSLContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        /**
         * Configures hostname verifier for the client.
         *
         * @param hostnameVerifier Hostname verifier to use
         * @return This builder for method chaining
         */
        public SafeClientBuilder hostnameVerifier(HostnameVerifier hostnameVerifier) {
            this.hostnameVerifier = hostnameVerifier;
            return this;
        }

        /**
         * Configures connection timeout.
         *
         * @param timeoutMs Connection timeout in milliseconds
         * @return This builder for method chaining
         */
        public SafeClientBuilder connectTimeout(int timeoutMs) {
            this.connectTimeoutMs = timeoutMs;
            return this;
        }

        /**
         * Configures read timeout.
         *
         * @param timeoutMs Read timeout in milliseconds
         * @return This builder for method chaining
         */
        public SafeClientBuilder readTimeout(int timeoutMs) {
            this.readTimeoutMs = timeoutMs;
            return this;
        }

        /**
         * Applies an existing ClientConfig, ensuring it gets MOXy-safe configuration.
         *
         * @param config Existing ClientConfig to enhance
         * @return This builder for method chaining
         */
        public SafeClientBuilder withConfig(ClientConfig config) {
            this.clientConfig = config;
            return this;
        }

        /**
         * Builds the configured Jersey client with MOXy prevention.
         *
         * @return A configured Jersey client safe from MOXy interference
         */
        public Client build() {
            return runWithJerseyClassLoader(() -> {
                if (clientConfig != null) {
                    applyAntiMoxyConfiguration(clientConfig);

                    if (!clientConfig.getProperties().containsKey(ClientProperties.CONNECT_TIMEOUT)) {
                        clientConfig.property(ClientProperties.CONNECT_TIMEOUT, connectTimeoutMs);
                    }
                    if (!clientConfig.getProperties().containsKey(ClientProperties.READ_TIMEOUT)) {
                        clientConfig.property(ClientProperties.READ_TIMEOUT, readTimeoutMs);
                    }

                    ClientBuilder builder = ClientBuilder.newBuilder().withConfig(clientConfig);

                    if (sslContext != null) {
                        builder.sslContext(sslContext);
                    }
                    if (hostnameVerifier != null) {
                        builder.hostnameVerifier(hostnameVerifier);
                    }

                    validateAntiMoxyConfiguration(clientConfig);

                    return builder.build();
                }

                return createSecureClient(sslContext, hostnameVerifier, connectTimeoutMs, readTimeoutMs);
            });
        }
    }

    /**
     * Runs Jersey client construction with the host (e.g. Solr webapp) classloader when the
     * Ranger plugin classloader is active, so Jersey types are not split across loaders.
     */
    public static <T> T runWithJerseyClassLoader(Callable<T> action) {
        ClassLoader jerseyCl = resolveJerseyClassLoader();
        ClassLoader previous = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(jerseyCl);
            return action.call();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build Jersey client using classloader " + jerseyCl, e);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    static ClassLoader resolveJerseyClassLoader() {
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();

        if (tccl != null && RANGER_PLUGIN_CLASSLOADER_CLASS.equals(tccl.getClass().getName())) {
            try {
                Method getPrev = tccl.getClass().getMethod("getPrevActiveClassLoader");
                ClassLoader hostCl = (ClassLoader) getPrev.invoke(tccl);

                if (hostCl != null) {
                    LOG.debug("Using host classloader from RangerPluginClassLoader: {}", hostCl);
                    return hostCl;
                }
            } catch (ReflectiveOperationException e) {
                LOG.debug("Could not resolve host classloader from RangerPluginClassLoader", e);
            }
        }

        ClassLoader fromJerseyApi = ClientConfig.class.getClassLoader();

        if (fromJerseyApi != null) {
            return fromJerseyApi;
        }

        return tccl != null ? tccl : RangerJersey2ClientBuilder.class.getClassLoader();
    }

    // ========== VALIDATION AND CONFIGURATION METHODS ==========

    /**
     * Validates that the calling code is using safe patterns.
     * Logs warnings for potentially unsafe usage.
     *
     * @param methodName Name of the method being called for logging
     */
    public static void validateSafeUsage(String className) {
        // This method can be extended to perform runtime validation
        // of calling patterns and log warnings for potentially unsafe usage
        LOG.debug("Safe Jersey client usage validated for: {}", className);
    }

    /**
     * Applies comprehensive anti-MOXy configuration to a ClientConfig.
     *
     * @param config ClientConfig to configure
     * @return The same ClientConfig for method chaining
     */
    public static ClientConfig applyAntiMoxyConfiguration(ClientConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("ClientConfig cannot be null");
        }

        LOG.debug("Applying anti-MOXy configuration to ClientConfig");

        registerJacksonProvider(config);

        // Disable Jersey's auto-discovery to prevent MOXy from being found and registered
        config.property(DISABLE_AUTO_DISCOVERY_PROPERTY, true);
        config.property(PROVIDER_SCANNING_RECURSIVE_PROPERTY, false);

        LOG.debug("Anti-MOXy configuration applied: Jackson registered, auto-discovery disabled");
        return config;
    }

    /**
     * Validates that anti-MOXy configuration is properly applied.
     *
     * @param config ClientConfig to validate
     * @return true if configuration is valid, false otherwise
     * @throws IllegalStateException if critical MOXy prevention measures are missing
     */
    public static boolean validateAntiMoxyConfiguration(ClientConfig config) {
        if (config == null) {
            LOG.warn("Cannot validate null ClientConfig");
            return false;
        }

        boolean isValid = true;

        // Check if auto-discovery is disabled
        Object autoDiscoveryValue = config.getProperty(DISABLE_AUTO_DISCOVERY_PROPERTY);
        if (autoDiscoveryValue == null || !Boolean.TRUE.equals(autoDiscoveryValue)) {
            LOG.error("CRITICAL: Jersey auto-discovery is not disabled! MOXy may be loaded.");
            isValid = false;
        }

        if (!isJacksonProviderRegistered(config)) {
            LOG.error("CRITICAL: Jackson JSON provider is not registered! Default JSON processing may fail.");
            isValid = false;
        }

        if (isValid) {
            LOG.debug("Anti-MOXy configuration validation passed");
        } else {
            throw new IllegalStateException("Critical MOXy prevention configuration is missing or invalid");
        }

        return isValid;
    }

    private static void registerJacksonProvider(ClientConfig config) {
        ClassLoader jerseyCl = config.getClass().getClassLoader();
        Exception           lastFailure = null;

        try {
            Class<?> featureClass = Class.forName(JERSEY_JACKSON_FEATURE_CLASS, true, jerseyCl);

            config.register(featureClass);
            LOG.debug("Registered {} from classloader {}", JERSEY_JACKSON_FEATURE_CLASS, jerseyCl);
            return;
        } catch (ReflectiveOperationException e) {
            lastFailure = e;
            LOG.debug("Jersey Jackson feature not available from {}: {}", jerseyCl, e.toString());
        }

        for (String providerClassName : JACKSON_JSON_PROVIDER_CLASSES) {
            try {
                Class<?> providerClass = Class.forName(providerClassName, true, jerseyCl);
                Object provider       = providerClass.getDeclaredConstructor().newInstance();

                config.register(provider);
                LOG.debug("Registered {} from classloader {}", providerClassName, jerseyCl);
                return;
            } catch (ReflectiveOperationException e) {
                lastFailure = e;
                LOG.debug("Jackson provider {} not available from {}: {}", providerClassName, jerseyCl, e.toString());
            }
        }

        throw new IllegalStateException("Failed to register a Jackson JSON provider from classloader " + jerseyCl, lastFailure);
    }

    private static boolean isJacksonProviderRegistered(ClientConfig config) {
        if (config.getClasses().stream().anyMatch(clazz -> JERSEY_JACKSON_FEATURE_CLASS.equals(clazz.getName()))) {
            return true;
        }

        for (String providerClassName : JACKSON_JSON_PROVIDER_CLASSES) {
            boolean registeredByClass = config.getClasses().stream()
                    .anyMatch(clazz -> providerClassName.equals(clazz.getName()));

            if (registeredByClass) {
                return true;
            }

            boolean registeredByInstance = config.getInstances().stream()
                    .anyMatch(instance -> providerClassName.equals(instance.getClass().getName()));

            if (registeredByInstance) {
                return true;
            }
        }

        return config.getClasses().stream().anyMatch(clazz -> clazz.getName().contains("jersey.jackson"))
                || config.getInstances().stream().anyMatch(instance -> instance.getClass().getName().contains("jersey.jackson"));
    }
}
