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

package org.apache.ranger.util;

import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.AppConstants;

import java.time.Instant;
import java.time.ZoneId;

/**
 * Builds the DB-flavor specific SQL expression (and the matching bind parameter) that converts a
 * UTC {@code create_time} column into the user's timezone and truncates it to a date, so day-based
 * audit metrics are bucketed in the requesting user's timezone.
 */
public final class TimezoneAdjustedDateUtil {
    private TimezoneAdjustedDateUtil() {
    }

    public static String getTimezoneAdjustedDateExpression(String column, ZoneId zoneId) {
        if (usesInlineOffset()) {
            return buildInlineOffsetExpression(column, zoneId);
        }

        return getBindBasedExpression(column);
    }

    /**
     * Returns the timezone bind value, or {@code null} when the offset is inlined in
     * {@link #getTimezoneAdjustedDateExpression(String, ZoneId)}.
     */
    public static Object getTimezoneParameter(ZoneId zoneId) {
        if (usesInlineOffset()) {
            return null;
        }

        return getOffsetMinutes(zoneId);
    }

    private static int getOffsetMinutes(ZoneId zoneId) {
        ZoneId effectiveZoneId = zoneId != null ? zoneId : ZoneId.of("UTC");

        return effectiveZoneId.getRules().getOffset(Instant.now()).getTotalSeconds() / 60;
    }

    private static boolean usesInlineOffset() {
        switch (RangerBizUtil.getDBFlavor()) {
            case AppConstants.DB_FLAVOR_ORACLE:
            case AppConstants.DB_FLAVOR_SQLSERVER:
            case AppConstants.DB_FLAVOR_SQLANYWHERE:
                return true;
            default:
                return false;
        }
    }

    private static String buildInlineOffsetExpression(String column, ZoneId zoneId) {
        int offsetMinutes = getOffsetMinutes(zoneId);

        switch (RangerBizUtil.getDBFlavor()) {
            case AppConstants.DB_FLAVOR_ORACLE:
                return "TRUNC(" + column + " + (" + offsetMinutes + " / 1440.0))";
            case AppConstants.DB_FLAVOR_SQLSERVER:
            case AppConstants.DB_FLAVOR_SQLANYWHERE:
                return "CAST(DATEADD(MINUTE, " + offsetMinutes + ", " + column + ") AS date)";
            default:
                throw new IllegalArgumentException("Unsupported DB flavor for timezone-adjusted date expression: "
                        + RangerBizUtil.getDBFlavorType(RangerBizUtil.getDBFlavor()));
        }
    }

    private static String getBindBasedExpression(String column) {
        switch (RangerBizUtil.getDBFlavor()) {
            case AppConstants.DB_FLAVOR_MYSQL:
                return "DATE(DATE_ADD(" + column + ", INTERVAL ? MINUTE))";
            case AppConstants.DB_FLAVOR_POSTGRES:
                return "((" + column + " + (? * INTERVAL '1 minute'))::date)";
            default:
                throw new IllegalArgumentException("Unsupported DB flavor for timezone-adjusted date expression: "
                        + RangerBizUtil.getDBFlavorType(RangerBizUtil.getDBFlavor()));
        }
    }
}
