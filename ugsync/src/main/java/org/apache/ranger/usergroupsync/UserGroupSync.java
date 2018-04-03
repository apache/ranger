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

 package org.apache.ranger.usergroupsync;


import org.apache.log4j.Logger;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;

public class UserGroupSync implements Runnable {
	
	private static final Logger LOG = Logger.getLogger(UserGroupSync.class);

	private boolean         shutdownFlag = false;
	private UserGroupSink   ugSink       = null;
	private UserGroupSource ugSource     =  null;



	public static void main(String[] args) {
		UserGroupSync userGroupSync = new UserGroupSync();
		userGroupSync.run();
	}

	public void run() {
		try {
			long sleepTimeBetweenCycleInMillis = UserGroupSyncConfig.getInstance().getSleepTimeInMillisBetweenCycle();

			boolean initDone = false;

			while (! initDone ) {
				try {
					ugSink = UserGroupSyncConfig.getInstance().getUserGroupSink();
					LOG.info("initializing sink: " + ugSink.getClass().getName());
					ugSink.init();

					ugSource = UserGroupSyncConfig.getInstance().getUserGroupSource();
					LOG.info("initializing source: " + ugSource.getClass().getName());
					ugSource.init();

					LOG.info("Begin: initial load of user/group from source==>sink");
					ugSource.updateSink(ugSink);
					LOG.info("End: initial load of user/group from source==>sink");

					initDone = true;

					LOG.info("Done initializing user/group source and sink");
				}
				catch(Throwable t) {
					LOG.error("Failed to initialize UserGroup source/sink. Will retry after " + sleepTimeBetweenCycleInMillis + " milliseconds. Error details: ", t);
					try {
						LOG.debug("Sleeping for [" + sleepTimeBetweenCycleInMillis + "] milliSeconds");
						Thread.sleep(sleepTimeBetweenCycleInMillis);
					} catch (Exception e) {
						LOG.error("Failed to wait for [" + sleepTimeBetweenCycleInMillis + "] milliseconds before attempting to initialize UserGroup source/sink", e);
					}
				}
			}

			while (! shutdownFlag ) {
				try {
					LOG.debug("Sleeping for [" + sleepTimeBetweenCycleInMillis + "] milliSeconds");
					Thread.sleep(sleepTimeBetweenCycleInMillis);
				} catch (InterruptedException e) {
					LOG.error("Failed to wait for [" + sleepTimeBetweenCycleInMillis + "] milliseconds before attempting to synchronize UserGroup information", e);
				}

				try {
					syncUserGroup();
				}
				catch(Throwable t) {
					LOG.error("Failed to synchronize UserGroup information. Error details: ", t);
				}
			}
		
		}
		catch(Throwable t) {
			LOG.error("UserGroupSync thread got an error", t);
		}
		finally {
			LOG.info("Shutting down the UserGroupSync thread");
		}
	}
	
	private void syncUserGroup() throws Throwable {
		UserGroupSyncConfig config = UserGroupSyncConfig.getInstance();

		try{
			if (config.isUserSyncEnabled()) {
				LOG.info("Begin: update user/group from source==>sink");
				ugSource.updateSink(ugSink);
				LOG.info("End: update user/group from source==>sink");
			}
		}catch(Throwable t){
			LOG.error("Failed to sync user/group : ", t);
		}
		
	}

}
