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
import org.apache.commons.lang.StringUtils;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Service class to handle log level management operations.
 * This class only supports Logback as the logging mechanism.
 */
@Component
public class RangerLogLevelService {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RangerLogLevelService.class);

    private static final String LOGBACK_CLASSIC_PREFIX = "ch.qos.logback.classic";

    /**
     * Sets the log level for a specific class or package.
     * @param loggerName The name of the logger (class or package name)
     * @param logLevel The log level to set (TRACE, DEBUG, INFO, WARN, ERROR, OFF)
     * @return A message indicating the result of the operation
     * @throws IllegalArgumentException if the log level is invalid
     * @throws UnsupportedOperationException if Logback is not the active logging framework
     */
    public String setLogLevel(String loggerName, String logLevel) {
        ILoggerFactory iLoggerFactory         = LoggerFactory.getILoggerFactory();
        String         loggerFactoryClassName = iLoggerFactory.getClass().getName();

        if (loggerFactoryClassName.startsWith(LOGBACK_CLASSIC_PREFIX)) {
            LOG.info("Setting log level for logger '{}' to '{}'", loggerName, logLevel);

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
            Level  level  = validateAndParseLogLevel(logLevel);
            Logger logger = getLogger(loggerName);

            logger.setLevel(level);

            LOG.info("Successfully set log level for logger '{}' to '{}'", loggerName, level);

            return String.format("Log level for logger '%s' has been set to '%s'", loggerName, level);
        } catch (Exception e) {
            LOG.error("Failed to set log level for logger '{}' to '{}'", loggerName, logLevel, e);

            throw new RuntimeException("Failed to set log level for logger '" + loggerName + "' to '" + logLevel + "'", e);
        }
    }

    private static Logger getLogger(String loggerName) {
        ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();

        if (!(iLoggerFactory instanceof LoggerContext)) {
            throw new IllegalStateException("Expected ILoggerFactory to be an instance of LoggerContext, but found " + iLoggerFactory.getClass().getName() + ". Is Logback configured as the SLF4J backend?");
        }

        LoggerContext context = (LoggerContext) iLoggerFactory;

        // Get or create the logger
        return context.getLogger(loggerName);
    }

    /**
     * Validates and parses the log level string.
     * @param logLevel The log level string to validate
     * @return The corresponding Logback Level object
     * @throws IllegalArgumentException if the log level is invalid
     */
    private Level validateAndParseLogLevel(String logLevel) {
        if (StringUtils.isBlank(logLevel)) {
            throw new IllegalArgumentException("Log level cannot be null or empty");
        }

        try {
            return Level.valueOf(logLevel.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid log level: '" + logLevel + "'. " + "Valid levels are: TRACE, DEBUG, INFO, WARN, ERROR, OFF");
        }
    }
}
