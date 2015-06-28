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

package org.apache.ranger.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Interface to get configuration values for Ranger (both security-admin and agents-common)
 */

public abstract class RangerConfigPropertyRepository {
	private static final Log LOG = LogFactory.getLog(RangerConfigPropertyRepository.class);


	protected static volatile RangerConfigPropertyRepository instance = null;

	public static String getProperty(String name) {

		String ret = null;

		if (instance != null) {
			ret = instance.getPropertyValue(name);
		} else {
			LOG.error("RangerConfigPropertyRepository.getPropery() - Object not created correctly.");
		}

		return ret;
	}

	abstract protected String getPropertyValue(String name);

}