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

package org.apache.ranger.metrics.source;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;

public abstract class RangerMetricsSource implements MetricsSource {

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
        refresh();
        update(collector, all);
    }

    /**
     * Responsibility of this method is to refresh metrics hold by this class.
     */
    abstract protected void refresh();

    /**
     * Responsibility of this method is to update metrics system with latest values of ranger metrics.
     * @param collector
     * @param all
     */
    abstract protected void update(MetricsCollector collector, boolean all);
}
