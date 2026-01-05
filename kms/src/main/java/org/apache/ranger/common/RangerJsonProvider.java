/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Priority;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.Provider;

@Provider
@Priority(1) // Highest priority to ensure this provider is selected over MOXy
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Component
public class RangerJsonProvider extends JacksonJaxbJsonProvider {
    private static final Logger LOG = LoggerFactory.getLogger(RangerJsonProvider.class);

    public RangerJsonProvider() {
        super(JsonUtilsV2.getMapper(), JacksonJaxbJsonProvider.DEFAULT_ANNOTATIONS);

        // Configure Jackson features to ensure robust JSON processing
        ObjectMapper mapper = JsonUtilsV2.getMapper();
        if (mapper != null) {
            // Ensure we can handle unknown properties gracefully
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }

        LOG.info("RangerJsonProvider() instantiated with Jackson JSON processing and enhanced configuration");
    }
}
