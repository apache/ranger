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

package org.apache.ranger.ha;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.ha.service.HARangerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ActiveInstanceElectorService implements HARangerService, LeaderLatchListener {

	private static final Logger LOG = LoggerFactory.getLogger(ActiveInstanceElectorService.class);

	private final Configuration configuration;
	private final ServiceState serviceState;
	private final ActiveInstanceState activeInstanceState;
	private Set<ActiveStateChangeHandler> activeStateChangeHandlerProviders;
	private List<ActiveStateChangeHandler> activeStateChangeHandlers;
	private CuratorFactory curatorFactory;
	private LeaderLatch leaderLatch;
	private String serverId;

	private static Long transitionCount   = 0L;
	private static Long lastTranstionTime = 0L;

	/**
	 * Create a new instance of {@link ActiveInstanceElectorService}
	 *
	 * @param activeStateChangeHandlerProviders The list of registered
	 *                                          {@link ActiveStateChangeHandler}s
	 *                                          that must be called back on state
	 *                                          changes.
	 * @throws Exception
	 */
	public ActiveInstanceElectorService(Set<ActiveStateChangeHandler> activeStateChangeHandlerProviders
						, CuratorFactory curatorFactory
						, ActiveInstanceState activeInstanceState
						, ServiceState serviceState
						, Configuration configuration
						)throws Exception {
		this.configuration = configuration;
		this.activeStateChangeHandlerProviders = activeStateChangeHandlerProviders;
		this.activeStateChangeHandlers = new ArrayList<>();
		this.curatorFactory = curatorFactory;
		this.activeInstanceState = activeInstanceState;
		this.serviceState = serviceState;
	}

	/**
	 * Join leader election on starting up.
	 *
	 * If service High Availability configuration is disabled, this operation is a
	 * no-op.
	 *
	 * @throws Exception
	 */
	@Override
	public void start() throws Exception {
		if (!HAConfiguration.isHAEnabled(configuration)) {
			LOG.info("HA is not enabled, no need to start leader election service");
			return;
		}
		cacheActiveStateChangeHandlers();
		serverId = RangerServiceServerIdSelector.selectServerId(configuration);
		joinElection();
	}

	private void joinElection() {
		LOG.info("Starting leader election for {}", serverId);
		String zkRoot = HAConfiguration.getZookeeperProperties(configuration).getZkRoot();
		leaderLatch = curatorFactory.leaderLatchInstance(serverId, zkRoot);
		leaderLatch.addListener(this);
		try {
			leaderLatch.start();
			LOG.info("Leader latch started for {}.",  serverId);
		} catch (Exception e) {
			LOG.info("Exception while starting leader latch for {}. " , serverId, e);
		}
	}

	/**
	 * Leave leader election process and clean up resources on shutting down.
	 *
	 * If service High Availability configuration is disabled, this operation is a
	 * no-op.
	 *
	 * @throws Exception
	 */
	@Override
	public void stop() throws Exception {
		if (!HAConfiguration.isHAEnabled(configuration)) {
			LOG.info("HA is not enabled, no need to stop leader election service");
			return;
		}
		try {
			leaderLatch.close();
			curatorFactory.close();
		} catch (IOException e) {
			LOG.error("Error closing leader latch", e);
		}
	}

	/**
	 * Call all registered {@link ActiveStateChangeHandler}s on being elected
	 * active.
	 *
	 * In addition, shared state information about this instance becoming active is
	 * updated using {@link ActiveInstanceState}.
	 */
	@Override
	public void isLeader() {
		LOG.warn("Server instance with server id {} is elected as leader ", serverId);
		serviceState.becomingActive();
		try {
			for (ActiveStateChangeHandler handler : activeStateChangeHandlers) {
				handler.instanceIsActive();
			}
			activeInstanceState.update(serverId);
			serviceState.setActive();
			transitionCount++;
			setLastTranstionTime(Instant.now().toEpochMilli());
		} catch (Exception e) {
			LOG.error("Got exception while activating", e);
			serviceState.setPassive();
			rejoinElection();
		}
	}


	private void cacheActiveStateChangeHandlers() {
		if (activeStateChangeHandlers.size() == 0) {
			activeStateChangeHandlers.addAll(activeStateChangeHandlerProviders);
			LOG.info("activeStateChangeHandlers(): before reorder: " + activeStateChangeHandlers);
		}
	}

	private void rejoinElection() {
		LOG.info(" inside rejoinElection ActiveInstanceElectorService");
		try {
			leaderLatch.close();
			joinElection();
		} catch (IOException e) {
			LOG.error("Error rejoining election", e);
		}
	}

	/*
	 * Call all registered {@link ActiveStateChangeHandler}s on becoming passive
	 * instance.
	 */
	@Override
	public void notLeader() {
		LOG.warn("Server instance with server id {} is removed as leader", serverId);
		serviceState.becomingPassive();

		for (int idx = activeStateChangeHandlers.size() - 1; idx >= 0; idx--) {
			try {
				activeStateChangeHandlers.get(idx).instanceIsPassive();
			} catch (Exception e) {
				LOG.error("Error while reacting to passive state.", e);
			}
		}

		serviceState.setPassive();
		transitionCount++;
		setLastTranstionTime(Instant.now().toEpochMilli());
	}

	public static Long getTransitionCount() {
		return transitionCount;
	}

	public static Long getLastTranstionTime() {
		return lastTranstionTime;
	}

	public static void setLastTranstionTime(Long lastTranstionTime) {
		ActiveInstanceElectorService.lastTranstionTime = lastTranstionTime;
	}

}
