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

package org.apache.ranger.rest;

import java.util.Objects;

public class ValidationFailureDetails {

	final String _fieldName;
	final String _subFieldName;
	final boolean _missing;
	final boolean _semanticError;
	final boolean _internalError;
	final String _reason;
	
	public ValidationFailureDetails(String fieldName, String subFieldName, boolean missing, boolean semanticError, boolean internalError, String reason) {
		_missing = missing;
		_semanticError = semanticError;
		_internalError = internalError;
		_fieldName = fieldName;
		_subFieldName = subFieldName;
		_reason = reason;
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
	
	@Override
	public String toString() {
		return String.format("Field[%s]%s is %s: reason[%s]", 
				_fieldName, 
				_subFieldName == null ? "" : ", subField[" + _subFieldName + "]",
				getType(), _reason);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(_fieldName, _subFieldName, _missing, _semanticError, _internalError, _reason);
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
				_semanticError == that._semanticError;
	}
}
