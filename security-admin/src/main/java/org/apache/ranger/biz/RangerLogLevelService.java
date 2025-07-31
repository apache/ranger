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

package org.apache.ranger.biz;

import java.lang.reflect.Method;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Service class to handle logging-related operations.
 * This class uses reflection to avoid compile-time dependencies on specific
 * logging frameworks, allowing it to be portable.
 */
@Component
public class RangerLogLevelService {

    private static final Logger LOG = LoggerFactory.getLogger(RangerLogLevelService.class);

    /**
     * Reloads the logging configuration by detecting the underlying SLF4J implementation.
     */
    public void reloadLogConfiguration() {
        ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
        String loggerFactoryClassName = iLoggerFactory.getClass().getName();

        LOG.info("Detected SLF4J binding: {}", loggerFactoryClassName);

        if (loggerFactoryClassName.startsWith("org.apache.logging.slf4j")) {
            reloadLog4j2Configuration();
        } else if (loggerFactoryClassName.startsWith("ch.qos.logback.classic")) {
            reloadLogbackConfiguration(iLoggerFactory);
        } else {
            String message = "Dynamic log configuration reload is not supported for SLF4J binding: " + loggerFactoryClassName;
            LOG.warn(message);
            throw new UnsupportedOperationException(message);
        }
    }

    /**
     * Reloads the Log4j 2 configuration using reflection.
     */
    private void reloadLog4j2Configuration() {
        try {
            LOG.debug("Attempting to reload Log4j 2 configuration via reflection.");
            Class<?> configuratorClass = Class.forName("org.apache.logging.log4j.core.config.Configurator");
            Method reconfigureMethod = configuratorClass.getMethod("reconfigure");
            reconfigureMethod.invoke(null); // Static method call
            LOG.info("Successfully triggered Log4j 2 configuration reload.");
        } catch (Exception e) {
            LOG.error("Failed to reload Log4j 2 configuration using reflection", e);
            throw new RuntimeException("Failed to reload Log4j 2 configuration", e);
        }
    }

    /**
     * Reloads the Logback configuration using reflection.
     */
    private void reloadLogbackConfiguration(ILoggerFactory iLoggerFactory) {
        try {
            LOG.debug("Attempting to reload Logback configuration via reflection.");
            // Get the context
            Object context = iLoggerFactory; // The ILoggerFactory is the LoggerContext in Logback

            // Create a JoranConfigurator instance
            Class<?> joranConfiguratorClass = Class.forName("ch.qos.logback.classic.joran.JoranConfigurator");
            Object configurator = joranConfiguratorClass.newInstance();

            // Set the context: configurator.setContext(context)
            Method setContextMethod = joranConfiguratorClass.getMethod("setContext", Class.forName("ch.qos.logback.core.Context"));
            setContextMethod.invoke(configurator, context);

            // Reset the context: context.reset()
            Method resetMethod = context.getClass().getMethod("reset");
            resetMethod.invoke(context);

            // Find the configuration file URL (e.g., logback.xml)
            java.net.URL configUrl = this.getClass().getClassLoader().getResource("logback.xml");
            if (configUrl == null) {
                configUrl = this.getClass().getClassLoader().getResource("logback-test.xml");
            }
            if (configUrl == null) {
                throw new RuntimeException("Could not find logback.xml or logback-test.xml on the classpath.");
            }

            // Configure: configurator.doConfigure(url)
            Method doConfigureMethod = joranConfiguratorClass.getMethod("doConfigure", java.net.URL.class);
            doConfigureMethod.invoke(configurator, configUrl);

            LOG.info("Successfully triggered Logback configuration reload from {}.", configUrl);
        } catch (Exception e) {
            LOG.error("Failed to reload Logback configuration using reflection", e);
            throw new RuntimeException("Failed to reload Logback configuration", e);
        }
    }
}
