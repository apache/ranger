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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class RangerTimeRangeChecker {
    private static final Logger LOG = LoggerFactory.getLogger(RangerTimeRangeChecker.class);

    private static final TimeZone                  DEFAULT_TIMEZONE   = TimeZone.getDefault();
    private static final ThreadLocal<DateFormat>   DATE_TIME_FORMAT   = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy/MM/dd HH:mm:ss"));
    private static final ThreadLocal<DateFormat>   DATE_TIME_FORMAT_2 = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy/MM/dd HH:mm"));
    private static final ThreadLocal<DateFormat>   DATE_FORMAT        = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy/MM/dd"));
    private static final ThreadLocal<DateFormat>[] DATE_FORMATS       = new ThreadLocal[] {DATE_TIME_FORMAT, DATE_TIME_FORMAT_2, DATE_FORMAT};

    private final long fromTime;
    private final long toTime;


    public RangerTimeRangeChecker(String fromTime, String toTime, String timeZone) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerTimeRangeChecker({}, {})", fromTime, toTime);
        }

        TimeZone tz = StringUtils.isNotBlank(timeZone) ? TimeZone.getTimeZone(timeZone) : null;;

        this.fromTime = parseDateTime(fromTime, tz);
        this.toTime   = parseDateTime(toTime, tz);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerTimeRangeChecker({}, {}): fromTime={}, toTime={}", fromTime, toTime, this.fromTime, this.toTime);
        }
    }

    public boolean isInRange(long time) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> isInRange({})", time);
        }

        final boolean ret;

        if (toTime < 0) { // IS_ACCESS_TIME_AFTER
            ret = time > fromTime;
        } else if (fromTime < 0) { // IS_ACCESS_TIME_BEFORE
            ret = time < toTime;
        } else { // IS_ACCESS_TIME_BETWEEN
            ret = time >= fromTime && time < toTime;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== isInRange({}): fromTime={}, toTime={} ret={}", time, fromTime, toTime, ret);
        }

        return ret;
    }

    private static long parseDateTime(String str, TimeZone tz) {
        final long ret;

        if (StringUtils.isNotBlank(str)) {
            Date           date = null;
            ParseException excp = null;

            for (ThreadLocal<DateFormat> formatter : DATE_FORMATS) {
                try {
                    date = formatter.get().parse(str);

                    break;
                } catch (ParseException exception) {
                    excp = exception;
                }
            }

            if (date != null) {
                ret = tz != null ? getAdjustedTime(date.getTime(), tz) : date.getTime();
            } else {
                LOG.error("Error parsing date:[{}]", str, excp);

                ret = -1;
            }
        } else {
            ret = -1;
        }

        return ret;
    }

    private static long getAdjustedTime(long localTime, TimeZone timeZone) {
        final long ret;

        if (!DEFAULT_TIMEZONE.equals(timeZone)) {
            int targetOffset  = timeZone.getOffset(localTime);
            int defaultOffset = DEFAULT_TIMEZONE.getOffset(localTime);
            int diff          = defaultOffset - targetOffset;

            ret = localTime + diff;
        } else {
            ret = localTime;
        }

        return ret;
    }
}
