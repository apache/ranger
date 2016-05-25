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
package org.apache.ranger.services.atlas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;

public class RangerServiceAtlas extends RangerBaseService {

    private static final Log LOG = LogFactory.getLog(RangerServiceAtlas.class);

    public RangerServiceAtlas() {
        super();
    }

    @Override
    public void init(RangerServiceDef serviceDef, RangerService service) {
        super.init(serviceDef, service);
    }

    @Override
    public HashMap<String, Object> validateConfig() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("This method will be implemented");
        }
        HashMap<String, Object> responseMap = new HashMap<String, Object>();
        String msg = "This feature is not available currently";
        BaseClient.generateResponseDataMap(true, msg, msg, null, null, responseMap);
        return responseMap;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        //This feature is not available currently
        return new ArrayList<String>();
    }
}
