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

package org.apache.ranger.audit.utils;

import org.slf4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility for logging in the audit-webapp.
 */
public class AuditServerLogFormatter {
    private AuditServerLogFormatter() {}

    /**
     * Log a map of logDetails at INFO level with a title
     * @param logger The logger to use
     * @param title The title/header for this log section
     * @param logDetails Map of key-value pairs to log
     */
    public static void logInfo(Logger logger, String title, Map<String, Object> logDetails) {
        if (logDetails != null && !logDetails.isEmpty()) {
            logger.info("{}:", title);
            logDetails.forEach((key, value) -> logger.info("  {} = [{}]", key, value));
        }
    }

    /**
     * Log a map of logDetails at DEBUG level with a title
     * @param logger The logger to use
     * @param title The title/header for this log section
     * @param logDetails Map of key-value pairs to log
     */
    public static void logDebug(Logger logger, String title, Map<String, Object> logDetails) {
        if (logDetails != null && !logDetails.isEmpty()) {
            logger.debug("{}:", title);
            logDetails.forEach((key, value) -> logger.debug("  {} = [{}]", key, value));
        }
    }

    /**
     * Log a map of logDetails at DEBUG level with a title
     * @param logger The logger to use
     * @param title The title/header for this log section
     * @param logDetails Map of key-value pairs to log
     */
    public static void logError(Logger logger, String title, Map<String, Object> logDetails) {
        if (logDetails != null && !logDetails.isEmpty()) {
            logger.error("{}:", title);
            logDetails.forEach((key, value) -> logger.error("  {} = [{}]", key, value));
        }
    }

    /**
     * Create a builder for constructing LogDetails maps to log
     * @param title The title for this log section
     * @return A new LogBuilder
     */
    public static LogBuilder builder(String title) {
        return new LogBuilder(title);
    }

    /**
     * Builder class for constructing structured log messages
     */
    public static class LogBuilder {
        private final String title;
        private final Map<String, Object> logDetails = new LinkedHashMap<>();

        private LogBuilder(String title) {
            this.title = title;
        }

        /**
         * Add a log key value pair
         * @param key
         * @param value
         * @return This builder for chaining
         */
        public LogBuilder add(String key, Object value) {
            logDetails.put(key, value);
            return this;
        }

        /**
         * Add a log only if the value is not null
         * @param key
         * @param value
         * @return This builder for chaining
         */
        public LogBuilder addIfNotNull(String key, Object value) {
            if (value != null) {
                logDetails.put(key, value);
            }
            return this;
        }

        /**
         * Log the accumulated LogDetails at INFO level
         * @param logger The logger to use
         */
        public void logInfo(Logger logger) {
            AuditServerLogFormatter.logInfo(logger, title, logDetails);
        }

        /**
         * Log the accumulated LogDetails at DEBUG level
         * @param logger The logger to use
         */
        public void logDebug(Logger logger) {
            AuditServerLogFormatter.logDebug(logger, title, logDetails);
        }

        /**
         * Log the accumulated LogDetails at DEBUG level
         * @param logger The logger to use
         */
        public void logError(Logger logger) {
            AuditServerLogFormatter.logError(logger, title, logDetails);
        }

        /**
         * Get the LogDetails map
         * @return The LogDetails map
         */
        public Map<String, Object> getLogDetails() {
            return new LinkedHashMap<>(logDetails);
        }
    }
}
