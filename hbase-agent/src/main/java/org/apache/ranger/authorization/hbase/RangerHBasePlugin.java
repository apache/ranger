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
package org.apache.ranger.authorization.hbase;

import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerHBasePlugin extends RangerBasePlugin {
    private static final Logger LOG = LoggerFactory.getLogger(RangerHBasePlugin.class);

    private boolean isHBaseShuttingDown;
    private boolean isColumnAuthOptimizationEnabled;

    public RangerHBasePlugin(String appType) {
        super("hbase", appType);
    }

    public void setHBaseShuttingDown(boolean hbaseShuttingDown) {
        isHBaseShuttingDown = hbaseShuttingDown;
    }

    @Override
    public void setPolicies(ServicePolicies policies) {
        super.setPolicies(policies);

        this.isColumnAuthOptimizationEnabled = Boolean.parseBoolean(this.getServiceConfigs().get(RangerHadoopConstants.HBASE_COLUMN_AUTH_OPTIMIZATION));

        LOG.info("isColumnAuthOptimizationEnabled={}", this.isColumnAuthOptimizationEnabled);
    }

    @Override
    public RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
        RangerAccessResult ret;

        if (isHBaseShuttingDown) {
            ret = new RangerAccessResult(RangerPolicy.POLICY_TYPE_ACCESS, this.getServiceName(), this.getServiceDef(), request);

            ret.setIsAllowed(true);
            ret.setIsAudited(false);

            LOG.warn("Auth request came after HBase shutdown....");
        } else {
            ret = super.isAccessAllowed(request, resultProcessor);
        }

        return ret;
    }

    public boolean getPropertyIsColumnAuthOptimizationEnabled() {
        return this.isColumnAuthOptimizationEnabled;
    }

    public void setColumnAuthOptimizationEnabled(boolean enable) {
        this.isColumnAuthOptimizationEnabled = enable;
    }
}
