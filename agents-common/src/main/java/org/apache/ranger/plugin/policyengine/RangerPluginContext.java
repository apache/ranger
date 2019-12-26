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

package org.apache.ranger.plugin.policyengine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.service.RangerBasePlugin;

public class RangerPluginContext {

	private static final Log LOG = LogFactory.getLog(RangerBasePlugin.class);
	private String clusterName;
	private String clusterType;

	public RangerPluginContext(String serviceType){
		this.clusterName = findClusterName(serviceType);
		this.clusterType = findClusterType(serviceType);
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getClusterType() {
		return clusterType;
	}

	public void setClusterType(String clusterType) {
		this.clusterType = clusterType;
	}

	private String findClusterName(String serviceType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPluginContext.findClusterName , serviceType = " + serviceType);
		}

		String propertyPrefix    = "ranger.plugin." + serviceType;
		String clusterName = RangerConfiguration.getInstance().get(propertyPrefix + ".access.cluster.name", "");
		if(StringUtil.isEmpty(clusterName)){
			clusterName = RangerConfiguration.getInstance().get(propertyPrefix + ".ambari.cluster.name", "");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPluginContext.findClusterName ");
		}

		return clusterName;
	}

	private String findClusterType(String serviceType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPluginContext.findClusterType , serviceType = " + serviceType);
		}

		String propertyPrefix    = "ranger.plugin." + serviceType;
		String clusterType = RangerConfiguration.getInstance().get(propertyPrefix + ".access.cluster.type", "");
		if(StringUtil.isEmpty(clusterType)){
			clusterType = RangerConfiguration.getInstance().get(propertyPrefix + ".ambari.cluster.type", "");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPluginContext.findClusterType ");
		}

		return clusterType;
	}

}
