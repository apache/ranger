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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.security.User;

public class HbaseUserUtilsImpl implements HbaseUserUtils {

	private static final Log LOG = LogFactory.getLog(HbaseUserUtilsImpl.class.getName());

	static Set<String> _SuperUsers = Collections.synchronizedSet(new HashSet<String>());
	static AtomicBoolean initialized = new AtomicBoolean(false);
	
	@Override
	public String getUserAsString(User user) {
		if (user == null) {
			throw new IllegalArgumentException("User is null!");
		}
		else {
			return user.getShortName();
		}
	}

	@Override
	public Set<String> getUserGroups(User user) {
		if (user == null) {
			throw new IllegalArgumentException("User is null!");
		}
		else {
			String[] groupsArray = user.getGroupNames();
			return new HashSet<String>(Arrays.asList(groupsArray));
		}
	}

	@Override
	public User getUser() {
		// current implementation does not use the request object!
		User user = RpcServer.getRequestUser();
		if (user == null) {
			try {
				user = User.getCurrent();
			} catch (IOException e) {
				LOG.error("Unable to get current user: User.getCurrent() threw IOException");
				user = null;
			}
		}
		return user;
	}


	@Override
	public String getUserAsString() {
		User user = getUser();
		if (user == null) {
			return "";
		}
		else {
			return getUserAsString(user);
		}
	}
}
