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

public class ValidationFailureDetailsBuilder {
    protected String  fieldName;
    protected boolean missing;
    protected boolean semanticError;
    protected String  reason;
    protected String  subFieldName;
    protected boolean internalError;
    protected int     errorCode;

    ValidationFailureDetailsBuilder becauseOf(String aReason) {
        reason = aReason;
        return this;
    }

    ValidationFailureDetailsBuilder isMissing() {
        missing = true;
        return this;
    }

    ValidationFailureDetailsBuilder isSemanticallyIncorrect() {
        semanticError = true;
        return this;
    }

    ValidationFailureDetailsBuilder field(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    ValidationFailureDetails build() {
        return new ValidationFailureDetails(errorCode, fieldName, subFieldName, missing, semanticError, internalError, reason);
    }

    ValidationFailureDetailsBuilder subField(String missingParameter) {
        subFieldName = missingParameter;
        return this;
    }

    ValidationFailureDetailsBuilder isAnInternalError() {
        internalError = true;
        return this;
    }

    ValidationFailureDetailsBuilder errorCode(int errorCode) {
        this.errorCode = errorCode;
        return this;
    }
}
