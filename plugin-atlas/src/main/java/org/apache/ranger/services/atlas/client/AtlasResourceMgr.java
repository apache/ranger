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

package org.apache.ranger.services.atlas.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.service.ResourceLookupContext;


public class AtlasResourceMgr {
	private static final Logger LOG = Logger.getLogger(AtlasResourceMgr.class);
	private static final String  ATLASTERM	= "terms";

	public static HashMap<String, Object> validateConfig(String serviceName, Map<String, String> configs) throws Exception {

		HashMap<String, Object> ret = null;

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> AtlasResourceMgr.validateConfig ServiceName: "+ serviceName + "Configs" + configs );
		}

		try {
			ret = AtlasClient.connectionTest(serviceName, configs);
		} catch (Exception e) {
			LOG.error("<== AtlasResourceMgr.validateConfig Error: " + e);
		  throw e;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== AtlasResourceMgr.validateConfig Result : "+ ret  );
		}
		return ret;
	}

    public static List<String> getAtlasTermResources(String serviceName, Map<String, String> configs,ResourceLookupContext context) {
           String userInput = context.getUserInput();
	   Map<String, List<String>> resourceMap = context.getResources();
	   List<String> resultList = null;
	   List<String> atlasTermList = null;
           String  atlasTermName = null;

		if ( resourceMap != null && !resourceMap.isEmpty() &&
			resourceMap.get(ATLASTERM) != null ) {
			atlasTermName = userInput;
			atlasTermList = resourceMap.get(ATLASTERM);
		} else {
			atlasTermName = userInput;
		}

        if (configs == null || configs.isEmpty()) {
                LOG.error("Connection Config is empty");
        } else {
               resultList = getAtlasTermResource(serviceName, configs, atlasTermName,atlasTermList);
        }
        return resultList;
    }

    public static List<String> getAtlasTermResource(String serviceName, Map<String, String> configs, String atlasTermName, List<String> atlasTermList) {
        final AtlasClient atlasClient = AtlasConnectionMgr.getAtlasClient(serviceName, configs);
        List<String> termList = null;
	    if (atlasClient != null) {
	       synchronized(atlasClient) {
               termList = atlasClient.getTermList(atlasTermName, atlasTermList);
             }
          }
       return termList;
     }
}
