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

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.hadoop.util.HttpExceptionUtils;
import org.springframework.stereotype.Component;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Invalid JSON format
 * We get the JSON request body which is not a valid json
 * Jersey provider(RangerJsonParserExceptionMapper) that converts Ranger Rest API exceptions into detailed HTTP errors.
 */
@Component
@Provider
public class RangerJsonParserExceptionMapper implements ExceptionMapper<JsonParseException> {
    @Override
    public Response toResponse(JsonParseException excp) {
        return HttpExceptionUtils.createJerseyExceptionResponse(Response.Status.BAD_REQUEST, excp);
    }
}
