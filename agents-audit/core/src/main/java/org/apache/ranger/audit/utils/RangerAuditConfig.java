/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.audit.utils;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

public class RangerAuditConfig {
    private static final    String            CORE_SITE_CONFIG_FILE = "core-site.xml";
    private static volatile RangerAuditConfig me;

    public static RangerAuditConfig getInstance() {
        RangerAuditConfig result = me;
        if (result == null) {
            synchronized (RangerAuditConfig.class) {
                result = me;
                if (result == null) {
                    result = new RangerAuditConfig();
                    me     = result;
                }
            }
        }
        return result;
    }

    public Configuration getConfig(Properties props) {
        XMLUtils.loadConfig(CORE_SITE_CONFIG_FILE, props);
        Configuration ret = new Configuration();
        for (String propName : props.stringPropertyNames()) {
            ret.set(propName, props.getProperty(propName));
        }
        return ret;
    }
}
