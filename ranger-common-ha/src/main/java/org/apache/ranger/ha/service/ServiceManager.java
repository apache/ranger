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

import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.ranger.ha.annotation.HAService;

@HAService
public class ServiceManager {

	private static final Logger LOG = LoggerFactory.getLogger(ServiceManager.class);
	private final List<HARangerService> services;

	public ServiceManager(List<HARangerService> services) {
		this.services = services;
		start();
		LOG.info("ServiceManager started with {} services", (services != null ? services.size() : 0));
	}

	@PostConstruct
	public void start() {
		LOG.info("ServiceManager.start() Starting services with service size :{} ", services.size());
		try {
			for (HARangerService svc : services) {

				LOG.info("Starting service {}", svc.getClass().getName());

				svc.start();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@PreDestroy
	public void stop() {
		for (int idx = services.size() - 1; idx >= 0; idx--) {
			HARangerService svc = services.get(idx);
			try {

				LOG.info("Stopping service {}", svc.getClass().getName());

				svc.stop();
			} catch (Throwable e) {
				LOG.warn("Error stopping service {}", svc.getClass().getName(), e);
			}
		}
	}

}
