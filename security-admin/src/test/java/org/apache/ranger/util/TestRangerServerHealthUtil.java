/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.util;

import org.apache.ranger.plugin.model.RangerServerHealth;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InjectMocks;

import java.util.Map;

import static org.apache.ranger.plugin.model.RangerServerHealth.RangerServerStatus.DOWN;

public class TestRangerServerHealthUtil {
    @InjectMocks
    RangerServerHealthUtil rangerServerHealthUtil = new RangerServerHealthUtil();

    @Test
    public void testGetRangerServerHealth() {
        RangerServerHealth rangerServerHealth = rangerServerHealthUtil.getRangerServerHealth("21.3c");
        Assert.assertEquals("RangerHealth.down()", DOWN, rangerServerHealth.getStatus());
        Assert.assertEquals("RangerHealth.getDetails()", 1, rangerServerHealth.getDetails().size());
        Assert.assertEquals("RangerHealth.getDetails('component')", 1, ((Map<?, ?>) rangerServerHealth.getDetails().get("components")).size());
    }
}
