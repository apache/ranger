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


import java.sql.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class RangerUdfMask extends RangerBaseUdf {
	private static final Log LOG = LogFactory.getLog(RangerUdfMask.class);

	private TransformerAdapter transformerAdapter = null;

	public RangerUdfMask() {
		LOG.debug("==> RangerUdfMask()");

		LOG.debug("<== RangerUdfMask()");
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerUdfMask.initialize(arguments.length=" + arguments.length + ")");
		}

		ObjectInspector ret = super.initialize(arguments);

		String transformType   = null;
		String transformerImpl = null;
		String transformerArgs = null;

		if(arguments.length > 1) { // transformType
			checkArgPrimitive(arguments, 1);

			if(arguments[1] instanceof WritableConstantStringObjectInspector) {
				Text value = ((WritableConstantStringObjectInspector)arguments[1]).getWritableConstantValue();

				if(value != null) {
					transformType = value.toString();
				}
			}
		}

		if(arguments.length > 2) { // transformerImpl
			checkArgPrimitive(arguments, 2);

			if(arguments[2] instanceof WritableConstantStringObjectInspector) {
				Text value = ((WritableConstantStringObjectInspector)arguments[2]).getWritableConstantValue();

				if(value != null) {
					transformerImpl = value.toString();
				}
			}
		}

		if(arguments.length > 3) { // transformerArgs
			checkArgPrimitive(arguments, 3);

			if(arguments[2] instanceof WritableConstantStringObjectInspector) {
				Text value = ((WritableConstantStringObjectInspector)arguments[3]).getWritableConstantValue();

				if(value != null) {
					transformerArgs = value.toString();
				}
			}
		}

		RangerTransformer transformer = RangerTransformerFactory.getTransformer(transformType, transformerImpl, transformerArgs);

		transformerAdapter = TransformerAdapter.getTransformerAdapter(columnType.getPrimitiveCategory(), transformer);

		if(LOG.isDebugEnabled()) {
			LOG.debug("getTransformerAdapter(category=" + columnType.getPrimitiveCategory() + ", transformer=" + transformer + "): " + transformerAdapter);

			LOG.debug("<== RangerUdfMask.initialize(arguments.length=" + arguments.length + "): ret=" + ret);
		}

		return ret;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerUdfMask.evaluate(arguments.length=" + arguments.length + ")");
		}

		final Object ret;

		if(transformerAdapter != null) {
			Object columnValue = columnType.getPrimitiveJavaObject(arguments[0].get());
			
			ret = transformerAdapter.getTransformedWritable(columnValue);
		} else {
			ret = columnType.getPrimitiveWritableObject(arguments[0].get());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerUdfMask.evaluate(arguments.length=" + arguments.length + "): ret=" + ret);
		}

		return ret;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("RangerUdfMask", children, ",");
	}
}

abstract class TransformerAdapter {
	final RangerTransformer transformer;

	TransformerAdapter(RangerTransformer transformer) {
		this.transformer = transformer;
	}

	abstract Object getTransformedWritable(Object value);

	static TransformerAdapter getTransformerAdapter(PrimitiveCategory category, RangerTransformer transformer) {
		TransformerAdapter ret = null;

		if(transformer != null) {
			switch(category) {
				case STRING:
					ret = new StringTransformerAdapter(transformer);
				break;

				case VARCHAR:
					ret = new VarCharTransformerAdapter(transformer);
				break;

				case SHORT:
					ret = new ShortTransformerAdapter(transformer);
				break;

				case INT:
					ret = new IntegerTransformerAdapter(transformer);
				break;

				case LONG:
					ret = new LongTransformerAdapter(transformer);
				break;

				case DATE:
					ret = new DateTransformerAdapter(transformer);
				break;

				default:
				break;
			}
		}

		return ret;
	}
}

class StringTransformerAdapter extends TransformerAdapter {
	final Text writable;

	public StringTransformerAdapter(RangerTransformer transformer) {
		this(transformer, new Text());
	}

	public StringTransformerAdapter(RangerTransformer transformer, Text writable) {
		super(transformer);

		this.writable = writable;
	}

	@Override
	public Object getTransformedWritable(Object value) {
		String transformedValue = transformer.transform((String)value);

		writable.set(transformedValue);

		return writable;
	}
}

class VarCharTransformerAdapter extends TransformerAdapter {
	final HiveVarcharWritable writable;

	public VarCharTransformerAdapter(RangerTransformer transformer) {
		this(transformer, new HiveVarcharWritable());
	}

	public VarCharTransformerAdapter(RangerTransformer transformer, HiveVarcharWritable writable) {
		super(transformer);

		this.writable = writable;
	}

	@Override
	public Object getTransformedWritable(Object value) {
		String transformedValue = transformer.transform(((HiveVarchar)value).getValue());

		writable.set(transformedValue);

		return writable;
	}
}

class ShortTransformerAdapter extends TransformerAdapter {
	final ShortWritable writable;

	public ShortTransformerAdapter(RangerTransformer transformer) {
		this(transformer, new ShortWritable());
	}

	public ShortTransformerAdapter(RangerTransformer transformer, ShortWritable writable) {
		super(transformer);

		this.writable = writable;
	}

	@Override
	public Object getTransformedWritable(Object value) {
		Short transformedValue = transformer.transform((Short)value);

		writable.set(transformedValue);

		return writable;
	}
}

class IntegerTransformerAdapter extends TransformerAdapter {
	final IntWritable writable;

	public IntegerTransformerAdapter(RangerTransformer transformer) {
		this(transformer, new IntWritable());
	}

	public IntegerTransformerAdapter(RangerTransformer transformer, IntWritable writable) {
		super(transformer);

		this.writable = writable;
	}

	@Override
	public Object getTransformedWritable(Object value) {
		Integer transformedValue = transformer.transform((Integer)value);

		writable.set(transformedValue);

		return writable;
	}
}

class LongTransformerAdapter extends TransformerAdapter {
	final LongWritable writable;

	public LongTransformerAdapter(RangerTransformer transformer) {
		this(transformer, new LongWritable());
	}

	public LongTransformerAdapter(RangerTransformer transformer, LongWritable writable) {
		super(transformer);

		this.writable = writable;
	}

	@Override
	public Object getTransformedWritable(Object value) {
		Long transformedValue = transformer.transform((Long)value);

		writable.set(transformedValue);

		return writable;
	}
}

class DateTransformerAdapter extends TransformerAdapter {
	final DateWritable writable;

	public DateTransformerAdapter(RangerTransformer transformer) {
		this(transformer, new DateWritable());
	}

	public DateTransformerAdapter(RangerTransformer transformer, DateWritable writable) {
		super(transformer);

		this.writable = writable;
	}

	@Override
	public Object getTransformedWritable(Object value) {
		Date transformedValue = transformer.transform((Date)value);

		writable.set(transformedValue);

		return writable;
	}
}
