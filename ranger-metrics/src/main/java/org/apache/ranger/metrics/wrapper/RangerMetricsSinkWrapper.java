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

package org.apache.ranger.metrics.wrapper;

import org.apache.hadoop.metrics2.MetricsSink;

public class RangerMetricsSinkWrapper {
    private final String name;
    private final String description;
    private final MetricsSink sink;

    public RangerMetricsSinkWrapper(String name, String description, MetricsSink sink) {
        this.name = name;
        this.description = description;
        this.sink=sink;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public MetricsSink getSink() {
        return sink;
    }
}
