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

/**
 * An interface that should be implemented by objects and services to react to
 * changes in state of an HMSToHdfsMetadataSynchronizer server.
 *
 * The two state transitions we handle are (1) becoming active and (2) becoming
 * passive.
 */
public interface ActiveStateChangeHandler {

	/**
	 * Callback that is invoked on an implementor when this instance of
	 * ranger component server is declared the leader.
	 *
	 * Any initialization that must be carried out by an implementor only when the
	 * server becomes active should happen on this callback.
	 *
	 * @throws {@link Exception} if anything is wrong on initialization
	 */
	void instanceIsActive() throws Exception;

	/**
	 * Callback that is invoked on an implementor when this instance of
	 * ranger component server is removed as the leader.
	 *
	 * Any cleanup that must be carried out by an implementor when the server
	 * becomes passive should happen on this callback.
	 *
	 * @throws {@link Exception} if anything is wrong on shutdown
	 */
	void instanceIsPassive() throws Exception;

}