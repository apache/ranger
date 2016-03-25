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

package org.apache.ranger.authorization.hive.udf;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


public abstract class RangerBaseUdf extends GenericUDF {
	private static final Log LOG = LogFactory.getLog(RangerBaseUdf.class);

	protected PrimitiveObjectInspector columnType = null;

	public RangerBaseUdf() {
		LOG.debug("==> RangerBaseUdf()");

		LOG.debug("<== RangerBaseUdf()");
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		LOG.debug("==> RangerBaseUdf.initialize()");

		ObjectInspector ret = null;

		checkArgPrimitive(arguments, 0); // column value

		columnType = ((PrimitiveObjectInspector)arguments[0]);

		ret = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(columnType.getPrimitiveCategory());

		LOG.debug("<== RangerBaseUdf.initialize()");

		return ret;
	}
}
