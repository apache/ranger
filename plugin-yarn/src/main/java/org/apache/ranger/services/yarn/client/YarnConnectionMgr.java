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

package org.apache.ranger.services.yarn.client;

import org.apache.log4j.Logger;


public class YarnConnectionMgr {

	public static final Logger LOG = Logger.getLogger(YarnConnectionMgr.class);
    
	public static YarnClient getYarnClient(final String yarnURL, String userName, String password) {
		YarnClient yarnClient = null;
        if (yarnURL == null || yarnURL.isEmpty()) {
        	LOG.error("Can not create YarnClient: yarnURL is empty");
        } else if (userName == null || userName.isEmpty()) {
        	LOG.error("Can not create YarnClient: YarnuserName is empty");
        } else if (password == null || password.isEmpty()) {
        	LOG.error("Can not create YarnClient: YarnPassWord is empty");
        } else {
            yarnClient =  new YarnClient(yarnURL, userName, password);
        }
        return yarnClient;
    }

}
