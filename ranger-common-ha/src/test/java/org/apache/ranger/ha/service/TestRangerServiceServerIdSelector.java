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

package org.apache.ranger.ha.service;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.ha.RangerServiceServerIdSelector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRangerServiceServerIdSelector {

    private static final Logger LOG = LoggerFactory.getLogger(TestRangerServiceServerIdSelector.class);
    private static Configuration conf = new Configuration();

    @BeforeAll
    public static void initialize(){
        conf.addResource(Resources.getResource("ranger-tagsync-site.xml"), true);
    }

    @Test
    public void testSelectServerId() {
        String serverId = null;
        try {
            LOG.info("test started");
            serverId = RangerServiceServerIdSelector.selectServerId(conf);
            Assertions.assertEquals("id1", serverId);
        }catch(Exception e){
            LOG.error(e.getMessage());

            }
    }

}
