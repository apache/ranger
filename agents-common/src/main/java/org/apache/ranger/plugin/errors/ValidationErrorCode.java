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
    // SERVICE VALIDATION
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

    // SERVICE-DEF VALIDATION
    SERVICE_DEF_VALIDATION_ERR_UNSUPPORTED_ACTION(2001, "Internal error: unsupported action[{0}]; isValid(Long) is only supported for DELETE"),
    SERVICE_DEF_VALIDATION_ERR_MISSING_FIELD(2002, "Internal error: missing field[{0}]"),
    SERVICE_DEF_VALIDATION_ERR_NULL_SERVICE_DEF_OBJECT(2003, "Internal error: service def object passed in was null"),
    SERVICE_DEF_VALIDATION_ERR_EMPTY_SERVICE_DEF_ID(2004, "Internal error: service def id was null/empty/blank"),
    SERVICE_DEF_VALIDATION_ERR_INVALID_SERVICE_DEF_ID(2005, "No service def found for id [{0}]"),
    SERVICE_DEF_VALIDATION_ERR_INVALID_SERVICE_DEF_NAME(2006, "Service def name[{0}] was null/empty/blank"),
    SERVICE_DEF_VALIDATION_ERR_SERVICE_DEF_NAME_CONFICT(2007, "service def with the name[{0}] already exists"),
    SERVICE_DEF_VALIDATION_ERR_ID_NAME_CONFLICT(2008, "id/name conflict: another service def already exists with name[{0}], its id is [{1}]"),
    SERVICE_DEF_VALIDATION_ERR_IMPLIED_GRANT_UNKNOWN_ACCESS_TYPE(2009, "implied grant[{0}] contains an unknown access types[{1}]"),
    SERVICE_DEF_VALIDATION_ERR_IMPLIED_GRANT_IMPLIES_ITSELF(2010, "implied grants list [{0}] for access type[{1}] contains itself"),
    SERVICE_DEF_VALIDATION_ERR_POLICY_CONDITION_NULL_EVALUATOR(2011, "evaluator on policy condition definition[{0}] was null/empty!"),
    SERVICE_DEF_VALIDATION_ERR_CONFIG_DEF_UNKNOWN_ENUM(2012, "subtype[{0}] of service def config[{1}] was not among defined enums[{2}]"),
    SERVICE_DEF_VALIDATION_ERR_CONFIG_DEF_UNKNOWN_ENUM_VALUE(2013, "default value[{0}] of service def config[{1}] was not among the valid values[{2}] of enums[{3}]"),
    SERVICE_DEF_VALIDATION_ERR_CONFIG_DEF_MISSING_TYPE(2014, "type of service def config[{0}] was null/empty"),
    SERVICE_DEF_VALIDATION_ERR_CONFIG_DEF_INVALID_TYPE(2015, "type[{0}] of service def config[{1}] is not among valid types: {2}"),
    SERVICE_DEF_VALIDATION_ERR_RESOURCE_GRAPH_INVALID(2016, "Resource graph implied by various resources, e.g. parent value is invalid.  Valid graph must forest (union of disjoint trees)."),
    SERVICE_DEF_VALIDATION_ERR_ENUM_DEF_NULL_OBJECT(2017, "Internal error: An enum def in enums collection is null"),
    SERVICE_DEF_VALIDATION_ERR_ENUM_DEF_NO_VALUES(2018, "enum [{0}] does not have any elements"),
    SERVICE_DEF_VALIDATION_ERR_ENUM_DEF_INVALID_DEFAULT_INDEX(2019, "default index[{0}] for enum [{1}] is invalid"),
    SERVICE_DEF_VALIDATION_ERR_ENUM_DEF_NULL_ENUM_ELEMENT(2020, "An enum element in enum element collection of enum [{0}] is null"),
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
