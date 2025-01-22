/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import jakarta.ws.rs.ext.ContextResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.ws.rs.ext.Provider;


@Provider
public class RangerJsonProvider implements ContextResolver<ObjectMapper> {
    private static final Logger LOG = LoggerFactory.getLogger(RangerJsonProvider.class);

    final ObjectMapper defaultMapper;

    public RangerJsonProvider() {
        defaultMapper = createDefaultMapper();
        LOG.info("RangerJsonProvider() instantiated");
    }

    @Override
    public ObjectMapper getContext(Class<?> aClass) {
        return defaultMapper;
    }

    private static ObjectMapper createDefaultMapper(){
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        return mapper;
    }
}
