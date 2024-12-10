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

package org.apache.ranger.plugin.model.validation;

import org.apache.ranger.plugin.errors.ValidationErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class ValidationFailureDetails {
    private static final Logger LOG = LoggerFactory.getLogger(ValidationFailureDetails.class);

    final String  fieldName;
    final String  subFieldName;
    final boolean missing;
    final boolean semanticError;
    final boolean internalError;
    final String  reason;
    final int     errorCode;

    public ValidationFailureDetails(ValidationErrorCode errorCode, String fieldName, Object... errorCodeArgs) {
        this(errorCode.getErrorCode(), fieldName, null, false, false, false, errorCode.getMessage(errorCodeArgs));
    }

    public ValidationFailureDetails(int errorCode, String fieldName, String subFieldName, boolean missing, boolean semanticError, boolean internalError, String reason) {
        this.errorCode     = errorCode;
        this.missing       = missing;
        this.semanticError = semanticError;
        this.internalError = internalError;
        this.fieldName     = fieldName;
        this.subFieldName  = subFieldName;
        this.reason        = reason;
    }

    public String getFieldName() {
        return fieldName;
    }

    public boolean isMissingRequiredValue() {
        return missing;
    }

    public boolean isSemanticallyIncorrect() {
        return semanticError;
    }

    public String getSubFieldName() {
        return subFieldName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, subFieldName, missing, semanticError, internalError, reason, errorCode);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ValidationFailureDetails)) {
            return false;
        }
        ValidationFailureDetails that = (ValidationFailureDetails) obj;

        return Objects.equals(fieldName, that.fieldName) &&
                Objects.equals(subFieldName, that.subFieldName) &&
                Objects.equals(reason, that.reason) &&
                internalError == that.internalError &&
                missing == that.missing &&
                semanticError == that.semanticError &&
                errorCode == that.errorCode;
    }

    @Override
    public String toString() {
        LOG.debug("ValidationFailureDetails.toString()");

        return String.format(" %s: error code[%d], reason[%s], field[%s], subfield[%s], type[%s]", "Validation failure",
                errorCode, reason, fieldName, subFieldName, getType());
    }

    String getType() {
        if (missing) {
            return "missing";
        }

        if (semanticError) {
            return "semantically incorrect";
        }

        if (internalError) {
            return "internal error";
        }

        return "";
    }
}
