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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URL;

/**
 * Service class to handle logging-related operations.
 * This class only supports Logback as the logging mechanism.
 */
@Component
public class RangerLogLevelService {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RangerLogLevelService.class);

    /**
     * Reloads the logging configuration using only Logback.
     * Any other logging implementation will result in an error.
     */
    public void reloadLogConfiguration() {
        ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
        String loggerFactoryClassName = iLoggerFactory.getClass().getName();

        LOG.info("Detected SLF4J binding: {}", loggerFactoryClassName);

        if (loggerFactoryClassName.startsWith("ch.qos.logback.classic")) {
            reloadLogbackConfiguration();
        } else {
            String message = "Logback is the only supported logging mechanism. Detected unsupported SLF4J binding: " + loggerFactoryClassName;
            LOG.error(message);
            throw new UnsupportedOperationException(message);
        }
    }

    /**
     * Sets the log level for a specific class or package.
     * 
     * @param loggerName The name of the logger (class or package name)
     * @param logLevel The log level to set (TRACE, DEBUG, INFO, WARN, ERROR, OFF)
     * @return A message indicating the result of the operation
     * @throws IllegalArgumentException if the log level is invalid
     * @throws UnsupportedOperationException if Logback is not the active logging framework
     */
    public String setLogLevel(String loggerName, String logLevel) {
        ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
        String loggerFactoryClassName = iLoggerFactory.getClass().getName();

        LOG.info("Setting log level for logger '{}' to '{}'", loggerName, logLevel);

        if (loggerFactoryClassName.startsWith("ch.qos.logback.classic")) {
            return setLogbackLogLevel(loggerName, logLevel);
        } else {
            String message = "Logback is the only supported logging mechanism. Detected unsupported SLF4J binding: " + loggerFactoryClassName;
            LOG.error(message);
            throw new UnsupportedOperationException(message);
        }
    }

    /**
     * Sets the Logback log level for a specific logger.
     */
    private String setLogbackLogLevel(String loggerName, String logLevel) {
        try {
            // Validate log level
            Level level = validateAndParseLogLevel(logLevel);
            
            // Get the Logback LoggerContext
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            
            // Get or create the logger
            Logger logger = context.getLogger(loggerName);
            
            // Set the log level
            logger.setLevel(level);
            
            // Log the change
            LOG.info("Successfully set log level for logger '{}' to '{}'", loggerName, level);
            
            return String.format("Log level for logger '%s' has been set to '%s'", loggerName, level);
            
        } catch (Exception e) {
            LOG.error("Failed to set log level for logger '{}' to '{}'", loggerName, logLevel, e);
            throw new RuntimeException("Failed to set log level for logger '" + loggerName + "' to '" + logLevel + "'", e);
        }
    }

    /**
     * Validates and parses the log level string.
     * 
     * @param logLevel The log level string to validate
     * @return The corresponding Logback Level object
     * @throws IllegalArgumentException if the log level is invalid
     */
    private Level validateAndParseLogLevel(String logLevel) {
        if (logLevel == null || logLevel.trim().isEmpty()) {
            throw new IllegalArgumentException("Log level cannot be null or empty");
        }
        
        String normalizedLevel = logLevel.trim().toUpperCase();
        
        try {
            return Level.valueOf(normalizedLevel);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid log level: '" + logLevel + "'. " +
                    "Valid levels are: TRACE, DEBUG, INFO, WARN, ERROR, OFF");
        }
    }

    /**
     * Reloads the Logback configuration using direct API calls.
     */
    private void reloadLogbackConfiguration() {
        try {
            LOG.debug("Attempting to reload Logback configuration.");
            
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            context.reset();

            // Find the configuration file URL (e.g., logback.xml)
            URL configUrl = this.getClass().getClassLoader().getResource("logback.xml");
            if (configUrl == null) {
                configUrl = this.getClass().getClassLoader().getResource("logback-test.xml");
            }
            if (configUrl == null) {
                throw new RuntimeException("Could not find logback.xml or logback-test.xml on the classpath.");
            }

            // Configure using the found configuration file
            configurator.doConfigure(configUrl);

            LOG.info("Successfully triggered Logback configuration reload from {}.", configUrl);
        } catch (JoranException e) {
            // Logback-specific error handling
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            StatusPrinter.printInCaseOfErrorsOrWarnings(context);
            LOG.error("Failed to reload Logback configuration", e);
            throw new RuntimeException("Failed to reload Logback configuration", e);
        } catch (Exception e) {
            LOG.error("Failed to reload Logback configuration", e);
            throw new RuntimeException("Failed to reload Logback configuration", e);
        }
    }
}
