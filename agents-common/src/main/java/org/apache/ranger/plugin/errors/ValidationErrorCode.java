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

package org.apache.ranger.plugin.errors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.MessageFormat;
import java.util.Arrays;

public enum ValidationErrorCode {

    SERVICE_VALIDATION_ERR_UNSUPPORTED_ACTION(1001, "Internal error: unsupported action[{0}]; isValid(Long) is only supported for DELETE"),
    SERVICE_VALIDATION_ERR_MISSING_FIELD(1002, "Internal error: missing field[{0}]"),
    SERVICE_VALIDATION_ERR_NULL_SERVICE_OBJECT(1003, "Internal error: service object passed in was null"),
    SERVICE_VALIDATION_ERR_EMPTY_SERVICE_ID(1004, "Internal error: service id was null/empty/blank"),
    SERVICE_VALIDATION_ERR_INVALID_SERVICE_ID(1005, "No service found for id [{0}]"),
    SERVICE_VALIDATION_ERR_INVALID_SERVICE_NAME(1006, "Service name[{0}] was null/empty/blank"),
    SERVICE_VALIDATION_ERR_SERVICE_NAME_CONFICT(1007, "service with the name[{0}] already exists"),
    SERVICE_VALIDATION_ERR_ID_NAME_CONFLICT(1008, "id/name conflict: another service already exists with name[{0}], its id is [{1}]"),
    SERVICE_VALIDATION_ERR_MISSING_SERVICE_DEF(1009, "service def [{0}] was null/empty/blank"),
    SERVICE_VALIDATION_ERR_INVALID_SERVICE_DEF(1010, "service def named[{0}] not found"),
    SERVICE_VALIDATION_ERR_REQUIRED_PARM_MISSING(1011, "required configuration parameter is missing; missing parameters: {0}"),
    ;


    private static final Log LOG = LogFactory.getLog(ValidationErrorCode.class);

    final int _errorCode;
    final String _template;

    ValidationErrorCode(int errorCode, String template) {
        _errorCode = errorCode;
        _template = template;
    }

    public String getMessage(Object... items) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("<== ValidationErrorCode.getMessage(%s)", Arrays.toString(items)));
        }

        MessageFormat mf = new MessageFormat(_template);
        String result = mf.format(items);

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("<== ValidationErrorCode.getMessage(%s): %s", Arrays.toString(items), result));
        }
        return result;
    }

    public int getErrorCode() {
        return _errorCode;
    }

    @Override
    public String toString() {
        return String.format("Code: %d, template: %s", _errorCode, _template);
    }
}
