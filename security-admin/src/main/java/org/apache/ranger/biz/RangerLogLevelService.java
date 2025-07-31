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

import java.net.URL;

import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

/**
 * Service class to handle logging-related operations.
 * This class is designed to be portable across different logging frameworks.
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

        if (loggerFactoryClassName.startsWith("org.apache.logging.slf4j")) { // More robust check for Log4j2
            reloadLog4j2Configuration();
        } else if (loggerFactoryClassName.startsWith("ch.qos.logback.classic")) { // Check for Logback
            reloadLogbackConfiguration();
        } else {
            String message = "Dynamic log configuration reload is not supported for SLF4J binding: " + loggerFactoryClassName;
            LOG.warn(message);
            throw new UnsupportedOperationException(message);
        }
    }

    /**
     * Reloads the Log4j 2 configuration.
     */
    private void reloadLog4j2Configuration() {
        try {
            Configurator.reconfigure();
            LOG.info("Successfully triggered Log4j 2 configuration reload.");
        } catch (Exception e) {
            LOG.error("Failed to reload Log4j 2 configuration", e);
            throw new RuntimeException("Failed to reload Log4j 2 configuration", e);
        }
    }

    /**
     * Reloads the Logback configuration.
     */
    private void reloadLogbackConfiguration() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

        try {
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            context.reset();

            // Logback's Joran will find and reload the default configuration file (e.g., logback.xml)
            configurator.doConfigure(this.getClass().getClassLoader().getResource("logback.xml"));
            LOG.info("Successfully triggered Logback configuration reload.");
        } catch (JoranException e) {
            // Logback-specific error handling
            StatusPrinter.printInCaseOfErrorsOrWarnings(context);
            LOG.error("Failed to reload Logback configuration", e);
            throw new RuntimeException("Failed to reload Logback configuration", e);
        }
    }
}
