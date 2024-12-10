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

package org.apache.ranger.plugin.conditionevaluator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RangerTimeOfDayMatcher extends RangerAbstractConditionEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(RangerTimeOfDayMatcher.class);

    // match "9am-5pm", "9 Am - 5 PM", "9 am.- 5 P.M", "9:30 AM - 4:00p.m." etc. spaces around - and after digits are allowed and dots in am/pm string in mixed cases is allowed
    static final Pattern PATTERN = Pattern.compile(" *(\\d{1,2})(:(\\d{1,2}))? *([aApP])\\.?[mM]\\.? *- *(\\d{1,2})(:(\\d{1,2}))? *([aApP])\\.?[mM]\\.? *");

    boolean           allowAny;
    final List<int[]> durations = new ArrayList<>();

    @Override
    public void init() {
        LOG.debug("==> RangerTimeOfDayMatcher.init({})", condition);

        super.init();

        if (condition == null) {
            LOG.debug("init: null policy condition! Will match always!");

            allowAny = true;
        } else if (CollectionUtils.isEmpty(condition.getValues())) {
            LOG.debug("init: empty conditions collection on policy condition!  Will match always!");

            allowAny = true;
        } else {
            for (String value : condition.getValues()) {
                if (StringUtils.isEmpty(value)) {
                    LOG.warn("init: Unexpected: one of the value in condition is null or empty!");
                } else {
                    int[] aDuration = extractDuration(value);

                    if (aDuration != null) {
                        durations.add(aDuration);
                    }
                }
            }
        }

        if (durations.isEmpty()) {
            LOG.debug("No valid durations found.  Will always match!");

            allowAny = true;
        }

        LOG.debug("<== RangerTimeOfDayMatcher.init({}): durations[{}]", condition, this);
    }

    @Override
    public boolean isMatched(RangerAccessRequest request) {
        LOG.debug("==> RangerTimeOfDayMatcher.isMatched({})", request);

        boolean matched = true;

        if (allowAny) {
            LOG.debug("isMatched: allowAny flag is true.  Matched!");
        } else if (request == null) {
            LOG.warn("isMatched: Unexpected: Request is null!  Implicitly matched!");
        } else if (request.getAccessTime() == null) {
            LOG.warn("isMatched: Unexpected: Accesstime on the request is null!  Implicitly matched!");
        } else {
            Date     date     = request.getAccessTime();
            Calendar calendar = GregorianCalendar.getInstance();

            calendar.setTime(date);

            int hourOfDay = calendar.get(Calendar.HOUR_OF_DAY);
            int minutes   = calendar.get(Calendar.MINUTE);

            if (!durationMatched(durations, hourOfDay, minutes)) {
                matched = false;

                LOG.debug("isMatched: None of the durations contains this hour of day[{}] and minutes[{}]", hourOfDay, minutes);
            }
        }

        LOG.debug("<== RangerTimeOfDayMatcher.isMatched({})): {}", request, matched);

        return matched;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("RangerTimeOfDayMatcher {");
        sb.append("allowAny=").append(allowAny).append(" ");
        sb.append("durations=[");
        for (int[] duration : durations) {
            sb.append("{start=").append(duration[0]).append("; end=").append(duration[1]).append("} ");
        }
        sb.append("]");
        sb.append("}");

        return sb.toString();
    }

    int[] extractDuration(String value) {
        LOG.debug("==> RangerTimeOfDayMatcher.extractDuration({})", value);

        int[] result = null;

        if (value == null) {
            LOG.warn("extractDuration: null input value!");
        } else {
            Matcher m = PATTERN.matcher(value);

            if (!m.matches()) {
                LOG.warn("extractDuration: input[{}] did not match pattern!", value);
            } else {
                int startHour = Integer.parseInt(m.group(1));
                int startMin  = 0;

                if (m.group(3) != null) {
                    startMin = Integer.parseInt(m.group(3));
                }

                String startType = m.group(4).toUpperCase();

                int endHour   = Integer.parseInt(m.group(5));
                int endMinute = 0;

                if (m.group(7) != null) {
                    endMinute = Integer.parseInt(m.group(7));
                }

                String endType = m.group(8).toUpperCase();

                if (startHour == 12) {
                    if ("A".equals(startType)) {
                        startHour = 0;
                    }
                } else if ("P".equals(startType)) {
                    startHour += 12;
                }

                if (endHour == 12) {
                    if ("A".equals(endType)) {
                        endHour = 0;
                    }
                } else if ("P".equals(endType)) {
                    endHour += 12;
                }

                result = new int[] {(startHour * 60) + startMin, (endHour * 60) + endMinute};
            }
        }

        LOG.debug("<== RangerTimeOfDayMatcher.extractDuration({}): duration:{}", value, Arrays.toString(result));

        return result;
    }

    boolean durationMatched(List<int[]> durations, int hourOfDay, int minutes) {
        for (int[] aDuration : durations) {
            int start        = aDuration[0];
            int end          = aDuration[1];
            int minutesOfDay = hourOfDay * 60 + minutes;

            if (start < end) {
                if (start <= minutesOfDay && minutesOfDay <= end) {
                    return true;
                }
            } else {
                if (start <= minutesOfDay || minutesOfDay <= end) {
                    return true;
                }
            }
        }
        return false;
    }
}
