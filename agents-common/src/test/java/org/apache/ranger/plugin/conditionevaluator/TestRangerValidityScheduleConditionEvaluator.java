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


import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyevaluator.RangerValidityScheduleEvaluator;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestRangerValidityScheduleConditionEvaluator {
    private static final String                   SERVICE_DEF_RESOURCE_PATH       = "/admin/service-defs/test-hive-servicedef.json";
    private static final RangerPolicyConditionDef VALIDITY_SCHEDULE_CONDITION_DEF = new RangerPolicyConditionDef(1L, "__validitySchedule", RangerValidityScheduleEvaluator.class.getName(), null);
    private static final RangerServiceDef         TEST_SERVICE_DEF                = initServiceDef();


    @Test
    public void testBothStartAndEndTime() {
        RangerValidityScheduleConditionEvaluator evaluator = getEvaluator("{ \"startTime\": \"2024/01/12 00:00:00\", \"endTime\": \"2024/01/13 00:00:00\" }");
        RangerAccessRequest                      request   = mock(RangerAccessRequest.class);

        // should match only for anytime on 2024/01/12
        when(request.getAccessTime()).thenReturn(getDate(2024, 1, 11, 9, 0, 0));
        assertFalse(evaluator.isMatched(request));

        when(request.getAccessTime()).thenReturn(getDate(2024, 1, 12, 9, 0, 0));
        assertTrue(evaluator.isMatched(request));

        when(request.getAccessTime()).thenReturn(getDate(2024, 1, 13, 9, 0, 0));
        assertFalse(evaluator.isMatched(request));
    }

    @Test
    public void testOnlyStartTime() {
        RangerValidityScheduleConditionEvaluator evaluator = getEvaluator("{ \"startTime\": \"2024/01/12 00:00:00\" }");
        RangerAccessRequest                      request   = mock(RangerAccessRequest.class);

        // should match only for anytime on or after 2024/01/12
        when(request.getAccessTime()).thenReturn(getDate(2024, 1, 11, 9, 0, 0));
        assertFalse(evaluator.isMatched(request));

        when(request.getAccessTime()).thenReturn(getDate(2024, 1, 12, 9, 0, 0));
        assertTrue(evaluator.isMatched(request));

        when(request.getAccessTime()).thenReturn(getDate(2024, 1, 13, 9, 0, 0));
        assertTrue(evaluator.isMatched(request));
    }

    @Test
    public void testOnlyEndTime() {
        RangerValidityScheduleConditionEvaluator evaluator = getEvaluator("{ \"endTime\": \"2024/01/13 00:00:00\" }");
        RangerAccessRequest                      request   = mock(RangerAccessRequest.class);

        // should match only for anytime before 2024/01/13
        when(request.getAccessTime()).thenReturn(getDate(2024, 1, 11, 9, 0, 0));
        assertTrue(evaluator.isMatched(request));

        when(request.getAccessTime()).thenReturn(getDate(2024, 1, 12, 9, 0, 0));
        assertTrue(evaluator.isMatched(request));

        when(request.getAccessTime()).thenReturn(getDate(2024, 1, 13, 9, 0, 0));
        assertFalse(evaluator.isMatched(request));
    }

    @Test
    public void testMultipleSchedules() {
        RangerValidityScheduleConditionEvaluator evaluator = getEvaluator("{ \"endTime\": \"2024/01/01 00:00:00\" }", "{ \"startTime\": \"2024/04/01 00:00:00\" }", "{ \"startTime\": \"2024/02/01 00:00:00\", \"endTime\": \"2024/02/15 00:00:00\" }");
        RangerAccessRequest                      request   = mock(RangerAccessRequest.class);

        // match following time periods:
        //  - anytime before      2024/01/01
        //  - anytime on or after 2024/04/01
        //  - anytime between     2024/02/01 - 2024/02/15
        when(request.getAccessTime()).thenReturn(getDate(2023, 12, 31, 9, 0, 0));
        assertTrue(evaluator.isMatched(request));

        when(request.getAccessTime()).thenReturn(getDate(2024, 4, 1, 9, 0, 0));
        assertTrue(evaluator.isMatched(request));

        when(request.getAccessTime()).thenReturn(getDate(2024, 2, 1, 9, 0, 0));
        assertTrue(evaluator.isMatched(request));

        when(request.getAccessTime()).thenReturn(getDate(2024, 2, 14, 9, 0, 0));
        assertTrue(evaluator.isMatched(request));

        when(request.getAccessTime()).thenReturn(getDate(2024, 1, 1, 9, 0, 0));
        assertFalse(evaluator.isMatched(request));

        when(request.getAccessTime()).thenReturn(getDate(2024, 3, 31, 9, 0, 0));
        assertFalse(evaluator.isMatched(request));
    }

    private RangerValidityScheduleConditionEvaluator getEvaluator(String...schedules) {
        RangerValidityScheduleConditionEvaluator evaluator = new RangerValidityScheduleConditionEvaluator();

        evaluator.setServiceDef(TEST_SERVICE_DEF);
        evaluator.setConditionDef(VALIDITY_SCHEDULE_CONDITION_DEF);
        evaluator.setPolicyItemCondition(new RangerPolicyItemCondition(VALIDITY_SCHEDULE_CONDITION_DEF.getName(), Arrays.asList(schedules)));

        evaluator.init();

        return evaluator;
    }

    private static Date getDate(int year, int month, int day, int hour, int min, int sec) {
        Calendar cal = Calendar.getInstance();

        cal.set(year, month - 1, day, hour, min, sec);

        return cal.getTime();
    }

    private static RangerServiceDef initServiceDef() throws RuntimeException {
        try (InputStream inStr = TestRangerValidityScheduleConditionEvaluator.class.getResourceAsStream(SERVICE_DEF_RESOURCE_PATH)) {
            RangerServiceDef serviceDef = inStr != null ? JsonUtils.jsonToObject(new InputStreamReader(inStr), RangerServiceDef.class) : null;

            if (serviceDef == null) {
                throw new RuntimeException("failed to load servicedef from " + SERVICE_DEF_RESOURCE_PATH);
            }

            Long maxId = serviceDef.getPolicyConditions().stream().map(RangerPolicyConditionDef::getItemId).filter(Objects::nonNull).max(Long::compareTo).orElse(0L);

            VALIDITY_SCHEDULE_CONDITION_DEF.setItemId(maxId + 1L);

            serviceDef.getPolicyConditions().add(VALIDITY_SCHEDULE_CONDITION_DEF);

            return serviceDef;
        } catch (IOException excp) {
            throw new RuntimeException(excp);
        }
    }
}
