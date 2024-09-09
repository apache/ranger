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

 package org.apache.ranger.view;

/**
 * Long
 *
 */

import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.view.ViewBaseBean;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class VXLong extends ViewBaseBean implements java.io.Serializable {
	private static final long serialVersionUID = 1L;


	/**
	 * Value
	 */
	protected long value;

	/**
	 * Default constructor. This will set all the attributes to default value.
	 */
	public VXLong ( ) {
	}

	/**
	 * This method sets the value to the member attribute <b>value</b>.
	 * You cannot set null to the attribute.
	 * @param value Value to set member attribute <b>value</b>
	 */
	public void setValue( long value ) {
		this.value = value;
	}

	/**
	 * Returns the value for the member attribute <b>value</b>
	 * @return long - value of member attribute <b>value</b>.
	 */
	public long getValue( ) {
		return this.value;
	}

	@Override
	public int getMyClassType( ) {
	    return AppConstants.CLASS_TYPE_LONG;
	}

	/**
	 * This return the bean content in string format
	 * @return formatedStr
	*/
	public String toString( ) {
		String str = "VXLong={";
		str += super.toString();
		str += "value={" + value + "} ";
		str += "}";
		return str;
	}
}
