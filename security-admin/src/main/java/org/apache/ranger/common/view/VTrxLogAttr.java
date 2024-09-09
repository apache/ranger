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

 package org.apache.ranger.common.view;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Objects;

import org.apache.ranger.common.AppConstants;
import org.apache.ranger.entity.XXAsset;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.util.RangerEnumUtil;

public class VTrxLogAttr extends ViewBaseBean implements Serializable{
	private static final long serialVersionUID = 1L;
	
	private final String  attribName;
	private final String  attribUserFriendlyName;
	private final boolean isEnum;
	private final boolean isObjName;

	public VTrxLogAttr(String attribName, String attribUserFriendlyName) {
		this(attribName, attribUserFriendlyName, false, false);
	}

	public VTrxLogAttr(String attribName, String attribUserFriendlyName, boolean isEnum) {
		this(attribName, attribUserFriendlyName, isEnum, false);
	}

	public VTrxLogAttr(String attribName, String attribUserFriendlyName, boolean isEnum, boolean isObjName) {
		super();

		this.attribName             = attribName;
		this.attribUserFriendlyName = attribUserFriendlyName;
		this.isEnum                 = isEnum;
		this.isObjName              = isObjName;
	}

	/**
	 * @return the attribName
	 */
	public String getAttribName() {
		return attribName;
	}

	/**
	 * @return the attribUserFriendlyName
	 */
	public String getAttribUserFriendlyName() {
		return attribUserFriendlyName;
	}

	/**
	 * @return the isEnum
	 */
	public boolean isEnum() {
		return isEnum;
	}

	/**
	 * @return the isObjName
	 */
	public boolean isObjName() {
		return isObjName;
	}

	@Override
	public int getMyClassType( ) {
	    return AppConstants.CLASS_TYPE_XA_TRANSACTION_LOG_ATTRIBUTE;
	}

	public String getAttrValue(Object obj, RangerEnumUtil xaEnumUtil) {
		String ret = null;

		if (obj != null) {
			Field field = getField(obj);

			if (field != null) {
				ret = getFieldValue(obj, field, xaEnumUtil);
			}
		}

		return ret;
	}

	@Override
	public String toString(){
		String str = "VTrxLogAttr={";
		str += super.toString();
		str += "attribName={" + attribName + "} ";
		str += "attribUserFriendlyName={" + attribUserFriendlyName + "} ";
		str += "isEnum={" + isEnum + "} ";
		str += "isObjName={" + isObjName + "} ";
		str += "}";
		return str;
	}

	private Field getField(Object obj) {
		Field field = null;

		try {
			field = obj.getClass().getDeclaredField(attribName);
		} catch (NoSuchFieldException excp) {
			try {
				field = obj.getClass().getSuperclass().getDeclaredField(attribName);
			} catch (NoSuchFieldException excp1) {
				// ignore
			}
		}

		if (field != null && !field.isAccessible()) {
			field.setAccessible(true);
		}

		return field;
	}

	private String getFieldValue(Object obj, Field field, RangerEnumUtil xaEnumUtil) {
		String ret = null;
		Object val = null;

		try {
			val = field.get(obj);
		} catch (IllegalArgumentException | IllegalAccessException excp) {
			// ignore
		}

		if (isEnum) {
			String enumName  = XXAsset.getEnumName(field.getName());
			int    enumValue = -1;

			if (val == null) {
				enumValue = 0;
			} else if (val instanceof Number) {
				enumValue = ((Number) val).intValue();
			} else {
				try {
					enumValue = Integer.parseInt(val.toString());
				} catch (Exception excp) {
					// ignore
				}
			}

			if (enumValue == -1) { // val is not a number
				ret = val.toString();
			} else {
				ret = xaEnumUtil.getLabel(enumName, enumValue);
			}
		} else if (val != null) {
			if (val instanceof String) {
				ret = (String) val;
			} else if (val instanceof Collection && ((Collection) val).isEmpty()) {
				ret = null;
			} else if (val instanceof Serializable) {
				try {
					ret = JsonUtilsV2.objToJson((Serializable) val);
				} catch (Exception excp) {
					// ignore
				}
			} else {
				ret = Objects.toString(val);
			}
		}

		return ret;
	}
}
