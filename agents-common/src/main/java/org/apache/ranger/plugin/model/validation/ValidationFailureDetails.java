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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ValidationFailureDetails {

	private static final Log LOG = LogFactory.getLog(ValidationFailureDetails.class);

	final String _fieldName;
	final String _subFieldName;
	final boolean _missing;
	final boolean _semanticError;
	final boolean _internalError;
	final String _reason;
	final int _errorCode;

	public ValidationFailureDetails(int errorCode, String fieldName, String subFieldName, boolean missing, boolean semanticError, boolean internalError, String reason) {
		_errorCode = errorCode;
		_missing = missing;
		_semanticError = semanticError;
		_internalError = internalError;
		_fieldName = fieldName;
		_subFieldName = subFieldName;
		_reason = reason;
	}

	// TODO - legacy signature remove after all 3 are ported over to new message framework
	public ValidationFailureDetails(String fieldName, String subFieldName, boolean missing, boolean semanticError, boolean internalError, String reason) {
		this(-1, fieldName, subFieldName, missing, semanticError, internalError, reason);
	}

	public String getFieldName() {
		return _fieldName;
	}

	public boolean isMissingRequiredValue() {
		return _missing;
	}

	public boolean isSemanticallyIncorrect() {
		return _semanticError;
	}
	
	String getType() {
		if (_missing) return "missing";
		if (_semanticError) return "semantically incorrect";
		if (_internalError) return "internal error";
		return "";
	}

	public String getSubFieldName() {
		return _subFieldName;
	}

	// matches "{blah}", "{{blah}", "{   }" and yields variables names like "blah", "{blah", "   ", etc. for substitution
	static final Pattern _Pattern = Pattern.compile("\\{([^\\}]+)\\}");

	public String substituteVariables(String template) {
		return template.replace("{field}", _fieldName == null ? "" : _fieldName)
				.replace("{sub-field}", _subFieldName == null ? "" : _subFieldName)
				.replace("{reason}", _reason == null ? "" : _reason);
	}

	// TODO legacy implementation.  Remove when all
	@Override
	public String toString() {
		LOG.debug("ValidationFailureDetails.toString()");
		return String.format("Field[%s]%s is %s: reason[%s]", 
				_fieldName, 
				_subFieldName == null ? "" : ", subField[" + _subFieldName + "]",
				getType(), _reason);
	}

	@Override
	public int hashCode() {
		return Objects.hash(_fieldName, _subFieldName, _missing, _semanticError, _internalError, _reason, _errorCode);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof ValidationFailureDetails)) {
			return false;
		}
		ValidationFailureDetails that = (ValidationFailureDetails)obj;
		return Objects.equals(_fieldName, that._fieldName) && 
				Objects.equals(_subFieldName, that._subFieldName) && 
				Objects.equals(_reason, that._reason) && 
				_internalError == that._internalError &&
				_missing == that._missing &&
				_semanticError == that._semanticError &&
				_errorCode == that._errorCode;
	}
}
