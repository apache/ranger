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

package org.apache.ranger;

import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.ha.ActiveInstanceState;
import org.apache.ranger.ha.CuratorFactory;
import org.apache.ranger.ha.ServiceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RangerHAInitializer {

	private static final Logger LOG = LoggerFactory.getLogger(RangerHAInitializer.class);

	protected ServiceState serviceState = null;
	protected CuratorFactory curatorFactory = null;
	protected ActiveInstanceState activeInstanceState = null;

	public RangerHAInitializer() {

	}

	public RangerHAInitializer(Configuration configuration) {
		try {
			init(configuration);
		} catch (Exception e) {
			LOG.error("RangerHAInitializer initialization failed",e.getMessage());
		}
	}

	public void init(Configuration configuration) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHAInitializer.init() initialization started ");
		}
		this.serviceState = ServiceState.getInstance(configuration);
		this.curatorFactory = CuratorFactory.getInstance(configuration);
		this.activeInstanceState = new ActiveInstanceState(configuration,this.curatorFactory);
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHAInitializer.init() initialization completed ");
		}
	}

	public abstract void stop() ;
}
