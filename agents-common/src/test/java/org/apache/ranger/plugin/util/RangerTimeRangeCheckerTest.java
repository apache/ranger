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

import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;
import java.util.TimeZone;

public class RangerTimeRangeCheckerTest {
    private static final String[] TIME_ZONES = { null, "GMT", "PDT" };

    @Test
    public void testAfterDate() {
        String baseTime = "2023/06/05";
        int    year     = 2023, month = 6, day = 5, hour = 0, min = 0, sec = 0;

        for (String timeZone : TIME_ZONES) {
            RangerTimeRangeChecker checker = new RangerTimeRangeChecker(baseTime, null, timeZone);
            TimeZone               tz      = timeZone == null ? TimeZone.getDefault() : TimeZone.getTimeZone(timeZone);

            // baseTime (should be *after*)
            long time = getTime(year, month, day, hour, min, sec, tz);
            Assert.assertFalse(toDateString(year, month, day, hour, min, sec, tz), checker.isInRange(time));

            // baseTime + 1 second
            time = getTime(year, month, day, hour, min, sec + 1, tz);
            Assert.assertTrue(toDateString(year, month, day, hour, min, sec + 1, tz), checker.isInRange(time));

            // baseTime - 1 second
            time = getTime(year, month, day, hour, min, sec - 1, tz);
            Assert.assertFalse(toDateString(year, month, day, hour, min, sec - 1, tz), checker.isInRange(time));
        }
    }

    @Test
    public void testAfterDateHHMM() {
        String baseTime = "2023/06/05 5:5";
        int    year     = 2023, month = 6, day = 5, hour = 5, min = 5, sec = 0;

        for (String timeZone : TIME_ZONES) {
            RangerTimeRangeChecker checker = new RangerTimeRangeChecker(baseTime, null, timeZone);
            TimeZone               tz      = timeZone == null ? TimeZone.getDefault() : TimeZone.getTimeZone(timeZone);

            // baseTime (should be *after*)
            long time = getTime(year, month, day, hour, min, sec, tz);
            Assert.assertFalse(toDateString(year, month, day, hour, min, sec, tz), checker.isInRange(time));

            // baseTime + 1 second
            time = getTime(year, month, day, hour, min, sec + 1, tz);
            Assert.assertTrue(toDateString(year, month, day, hour, min, sec + 1, tz), checker.isInRange(time));

            // baseTime - 1 second
            time = getTime(year, month, day, hour, min, sec - 1, tz);
            Assert.assertFalse(toDateString(year, month, day, hour, min, sec - 1, tz), checker.isInRange(time));
        }
    }

    @Test
    public void testAfterDateHHMMss() {
        String baseTime = "2023/06/05 5:5:5";
        int    year     = 2023, month = 6, day = 5, hour = 5, min = 5, sec = 5;

        for (String timeZone : TIME_ZONES) {
            RangerTimeRangeChecker checker = new RangerTimeRangeChecker(baseTime, null, timeZone);
            TimeZone               tz      = timeZone == null ? TimeZone.getDefault() : TimeZone.getTimeZone(timeZone);

            // baseTime (should be *after*)
            long time = getTime(year, month, day, hour, min, sec, tz);
            Assert.assertFalse(toDateString(year, month, day, hour, min, sec, tz), checker.isInRange(time));

            // baseTime + 1 second
            time = getTime(year, month, day, hour, min, sec + 1, tz);
            Assert.assertTrue(toDateString(year, month, day, hour, min, sec + 1, tz), checker.isInRange(time));

            // baseTime - 1 second
            time = getTime(year, month, day, hour, min, sec - 1, tz);
            Assert.assertFalse(toDateString(year, month, day, hour, min, sec - 1, tz), checker.isInRange(time));
        }
    }

    @Test
    public void testBeforeDate() {
        String baseTime = "2023/07/05";
        int    year     = 2023, month = 7, day = 5, hour = 0, min = 0, sec = 0;

        for (String timeZone : TIME_ZONES) {
            RangerTimeRangeChecker checker = new RangerTimeRangeChecker(null, baseTime, timeZone);
            TimeZone               tz      = timeZone == null ? TimeZone.getDefault() : TimeZone.getTimeZone(timeZone);

            // baseTime (should be *before*)
            long time = getTime(year, month, day, hour, min, sec, tz);
            Assert.assertFalse(toDateString(year, month, day, hour, min, sec, tz), checker.isInRange(time));

            // baseTime + 1 second
            time = getTime(year, month, day, hour, min, sec + 1, tz);
            Assert.assertFalse(toDateString(year, month, day, hour, min, sec + 1, tz), checker.isInRange(time));

            // baseTime - 1 second
            time = getTime(year, month, day, hour, min, sec - 1, tz);
            Assert.assertTrue(toDateString(year, month, day, hour, min, sec - 1, tz), checker.isInRange(time));
        }
    }

    @Test
    public void testBeforeDateHHMM() {
        String baseTime = "2023/07/05 5:5";
        int    year     = 2023, month = 7, day = 5, hour = 5, min = 5, sec = 0;

        for (String timeZone : TIME_ZONES) {
            RangerTimeRangeChecker checker = new RangerTimeRangeChecker(null, baseTime, timeZone);
            TimeZone               tz      = timeZone == null ? TimeZone.getDefault() : TimeZone.getTimeZone(timeZone);

            // baseTime (should be *before*)
            long time = getTime(year, month, day, hour, min, sec, tz);
            Assert.assertFalse(toDateString(year, month, day, hour, min, sec, tz), checker.isInRange(time));

            // baseTime + 1 second
            time = getTime(year, month, day, hour, min, sec + 1, tz);
            Assert.assertFalse(toDateString(year, month, day, hour, min, sec + 1, tz), checker.isInRange(time));

            // baseTime - 1 second
            time = getTime(year, month, day, hour, min, sec - 1, tz);
            Assert.assertTrue(toDateString(year, month, day, hour, min, sec - 1, tz), checker.isInRange(time));
        }
    }

    @Test
    public void testBeforeDateHHMMss() {
        String baseTime = "2023/07/05 5:5:5";
        int    year     = 2023, month = 7, day = 5, hour = 5, min = 5, sec = 5;

        for (String timeZone : TIME_ZONES) {
            RangerTimeRangeChecker checker = new RangerTimeRangeChecker(null, baseTime, timeZone);
            TimeZone               tz      = timeZone == null ? TimeZone.getDefault() : TimeZone.getTimeZone(timeZone);

            // baseTime (should be *before*)
            long time = getTime(year, month, day, hour, min, sec, tz);
            Assert.assertFalse(toDateString(year, month, day, hour, min, sec, tz), checker.isInRange(time));

            // baseTime + 1 second
            time = getTime(year, month, day, hour, min, sec + 1, tz);
            Assert.assertFalse(toDateString(year, month, day, hour, min, sec + 1, tz), checker.isInRange(time));

            // baseTime - 1 second
            time = getTime(year, month, day, hour, min, sec - 1, tz);
            Assert.assertTrue(toDateString(year, month, day, hour, min, sec - 1, tz), checker.isInRange(time));
        }
    }

    @Test
    public void testBetweenDate() {
        String fromTime = "2023/06/05";
        String toTIme   = "2023/07/05";
        int    fromYear = 2023, fromMonth = 6, fromDay = 5, fromHour = 0, fromMin = 0, fromSec = 0;
        int    toYear   = 2023, toMonth   = 7, toDay   = 5, toHour   = 0, toMin   = 0, toSec   = 0;

        for (String timeZone : TIME_ZONES) {
            RangerTimeRangeChecker checker = new RangerTimeRangeChecker(fromTime, toTIme, timeZone);
            TimeZone               tz      = timeZone == null ? TimeZone.getDefault() : TimeZone.getTimeZone(timeZone);

            // fromTime (should be *on or after*)
            long time = getTime(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec, tz);
            Assert.assertTrue(toDateString(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec, tz), checker.isInRange(time));

            // fromTime + 1 second
            time = getTime(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec + 1, tz);
            Assert.assertTrue(toDateString(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec + 1, tz), checker.isInRange(time));

            // fromTime - 1 second
            time = getTime(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec - 1, tz);
            Assert.assertFalse(toDateString(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec - 1, tz), checker.isInRange(time));

            // toTime (should be *before*)
            time = getTime(toYear, toMonth, toDay, toHour, toMin, toSec, tz);
            Assert.assertFalse(toDateString(toYear, toMonth, toDay, toHour, toMin, toSec, tz), checker.isInRange(time));

            // toTime + 1 second
            time = getTime(toYear, toMonth, toDay, toHour, toMin, toSec + 1, tz);
            Assert.assertFalse(toDateString(toYear, toMonth, toDay, toHour, toMin, toSec + 1, tz), checker.isInRange(time));

            // toTime - 1 second
            time = getTime(toYear, toMonth, toDay, toHour, toMin, toSec - 1, tz);
            Assert.assertTrue(toDateString(toYear, toMonth, toDay, toHour, toMin, toSec - 1, tz), checker.isInRange(time));
        }
    }

    @Test
    public void testBetweenDateHHMM() {
        String fromTime = "2023/06/05 5:5";
        String toTIme   = "2023/07/05 5:5";
        int    fromYear = 2023, fromMonth = 6, fromDay = 5, fromHour = 5, fromMin = 5, fromSec = 0;
        int    toYear   = 2023, toMonth   = 7, toDay   = 5, toHour   = 5, toMin   = 5, toSec   = 0;

        for (String timeZone : TIME_ZONES) {
            RangerTimeRangeChecker checker = new RangerTimeRangeChecker(fromTime, toTIme, timeZone);
            TimeZone               tz      = timeZone == null ? TimeZone.getDefault() : TimeZone.getTimeZone(timeZone);

            // fromTime (should be *on or after*)
            long time = getTime(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec, tz);
            Assert.assertTrue(toDateString(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec, tz), checker.isInRange(time));

            // fromTime + 1 second
            time = getTime(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec + 1, tz);
            Assert.assertTrue(toDateString(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec + 1, tz), checker.isInRange(time));

            // fromTime - 1 second
            time = getTime(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec - 1, tz);
            Assert.assertFalse(toDateString(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec - 1, tz), checker.isInRange(time));

            // toTime (should be *before*)
            time = getTime(toYear, toMonth, toDay, toHour, toMin, toSec, tz);
            Assert.assertFalse(toDateString(toYear, toMonth, toDay, toHour, toMin, toSec, tz), checker.isInRange(time));

            // toTime + 1 second
            time = getTime(toYear, toMonth, toDay, toHour, toMin, toSec + 1, tz);
            Assert.assertFalse(toDateString(toYear, toMonth, toDay, toHour, toMin, toSec + 1, tz), checker.isInRange(time));

            // toTime - 1 second
            time = getTime(toYear, toMonth, toDay, toHour, toMin, toSec - 1, tz);
            Assert.assertTrue(toDateString(toYear, toMonth, toDay, toHour, toMin, toSec - 1, tz), checker.isInRange(time));
        }
    }

    @Test
    public void testBetweenDateHHMMss() {
        String fromTime = "2023/06/05 5:5:5";
        String toTIme   = "2023/07/05 5:5:5";
        int    fromYear = 2023, fromMonth = 6, fromDay = 5, fromHour = 5, fromMin = 5, fromSec = 5;
        int    toYear   = 2023, toMonth   = 7, toDay   = 5, toHour   = 5, toMin   = 5, toSec   = 5;

        for (String timeZone : TIME_ZONES) {
            RangerTimeRangeChecker checker = new RangerTimeRangeChecker(fromTime, toTIme, timeZone);
            TimeZone               tz      = timeZone == null ? TimeZone.getDefault() : TimeZone.getTimeZone(timeZone);

            // fromTime (should be *on or after*)
            long time = getTime(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec, tz);
            Assert.assertTrue(toDateString(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec, tz), checker.isInRange(time));

            // fromTime + 1 second
            time = getTime(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec + 1, tz);
            Assert.assertTrue(toDateString(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec + 1, tz), checker.isInRange(time));

            // fromTime - 1 second
            time = getTime(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec - 1, tz);
            Assert.assertFalse(toDateString(fromYear, fromMonth, fromDay, fromHour, fromMin, fromSec - 1, tz), checker.isInRange(time));

            // toTime (should be *before*)
            time = getTime(toYear, toMonth, toDay, toHour, toMin, toSec, tz);
            Assert.assertFalse(toDateString(toYear, toMonth, toDay, toHour, toMin, toSec, tz), checker.isInRange(time));

            // toTime + 1 second
            time = getTime(toYear, toMonth, toDay, toHour, toMin, toSec + 1, tz);
            Assert.assertFalse(toDateString(toYear, toMonth, toDay, toHour, toMin, toSec + 1, tz), checker.isInRange(time));

            // toTime - 1 second
            time = getTime(toYear, toMonth, toDay, toHour, toMin, toSec - 1, tz);
            Assert.assertTrue(toDateString(toYear, toMonth, toDay, toHour, toMin, toSec - 1, tz), checker.isInRange(time));
        }
    }

    private long getTime(int year, int month, int day, int hour, int minute, int sec, TimeZone tz) {
        Calendar cal = new Calendar.Builder().setDate(year, month - 1, day).setTimeOfDay(hour, minute, sec).setTimeZone(tz).build();

        return cal.getTime().getTime();
    }

    private static String toDateString(int year, int month, int day, int hour, int minute, int sec, TimeZone tz) {
        Calendar cal = new Calendar.Builder().setDate(year, month - 1, day).setTimeOfDay(hour, minute, sec).setTimeZone(tz).build();

        return cal.getTime().toString();
    }
}
