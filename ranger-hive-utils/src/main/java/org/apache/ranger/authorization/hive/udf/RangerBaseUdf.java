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
// org.apache.hadoop.hive.ql.udf.generic.


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.sql.Date;


public abstract class RangerBaseUdf extends GenericUDF {
	private static final Log LOG = LogFactory.getLog(RangerBaseUdf.class);

	final protected RangerBaseUdf.RangerTransformer transformer;
	protected RangerTransformerAdapter transformerAdapter = null;

	protected RangerBaseUdf(RangerBaseUdf.RangerTransformer transformer) {
		this.transformer = transformer;
	}

	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		LOG.debug("==> RangerBaseUdf.initialize()");

		checkArgPrimitive(arguments, 0); // first argument is the column to be transformed

		PrimitiveObjectInspector columnType = ((PrimitiveObjectInspector) arguments[0]);

		transformer.init(arguments, 1);

		transformerAdapter = RangerTransformerAdapter.getTransformerAdapter(columnType, transformer);

		ObjectInspector ret = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(columnType.getPrimitiveCategory());

		LOG.debug("<== RangerBaseUdf.initialize()");

		return ret;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		return transformerAdapter.getTransformedWritable(arguments[0]);
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString(getClass().getName(), children, ",");
	}

	static abstract class RangerTransformer {
		abstract void    init(ObjectInspector[] arguments, int startIdx);

		abstract String  transform(String value);
		abstract Byte    transform(Byte value);
		abstract Short   transform(Short value);
		abstract Integer transform(Integer value);
		abstract Long    transform(Long value);
		abstract Date    transform(Date value);
	}
}


abstract class RangerTransformerAdapter {
	final RangerBaseUdf.RangerTransformer transformer;

	RangerTransformerAdapter(RangerBaseUdf.RangerTransformer transformer) {
		this.transformer = transformer;
	}

	abstract Object getTransformedWritable(DeferredObject value) throws HiveException;

	static RangerTransformerAdapter getTransformerAdapter(PrimitiveObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer) {
		final RangerTransformerAdapter ret;

		switch(columnType.getPrimitiveCategory()) {
			case STRING:
				ret = new StringTransformerAdapter((StringObjectInspector)columnType, transformer);
				break;

			case VARCHAR:
				ret = new HiveVarcharTransformerAdapter((HiveVarcharObjectInspector)columnType, transformer);
				break;

			case CHAR:
				ret = new HiveCharTransformerAdapter((HiveCharObjectInspector)columnType, transformer);
				break;

			case BYTE:
				ret = new ByteTransformerAdapter((ByteObjectInspector)columnType, transformer);
				break;

			case SHORT:
				ret = new ShortTransformerAdapter((ShortObjectInspector)columnType, transformer);
				break;

			case INT:
				ret = new IntegerTransformerAdapter((IntObjectInspector)columnType, transformer);
				break;

			case LONG:
				ret = new LongTransformerAdapter((LongObjectInspector)columnType, transformer);
				break;

			case DATE:
				ret = new DateTransformerAdapter((DateObjectInspector)columnType, transformer);
				break;

			default:
				ret = new NoTransformAdapter(columnType, transformer);
				break;
		}

		return ret;
	}
}

class ByteTransformerAdapter extends RangerTransformerAdapter {
	final ByteObjectInspector columnType;
	final ByteWritable        writable;

	public ByteTransformerAdapter(ByteObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer) {
		this(columnType, transformer, new ByteWritable());
	}

	public ByteTransformerAdapter(ByteObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer, ByteWritable writable) {
		super(transformer);

		this.columnType = columnType;
		this.writable   = writable;
	}

	@Override
	public Object getTransformedWritable(DeferredObject object) throws HiveException {
		Byte value = (Byte)columnType.getPrimitiveJavaObject(object.get());

		if(value != null) {
			Byte transformedValue = transformer.transform(value);

			writable.set(transformedValue);

			return writable;
		}

		return null;
	}
}

class DateTransformerAdapter extends RangerTransformerAdapter {
	final DateObjectInspector columnType;
	final DateWritable        writable;

	public DateTransformerAdapter(DateObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer) {
		this(columnType, transformer, new DateWritable());
	}

	public DateTransformerAdapter(DateObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer, DateWritable writable) {
		super(transformer);

		this.columnType = columnType;
		this.writable   = writable;
	}

	@Override
	public Object getTransformedWritable(DeferredObject object) throws HiveException {
		Date value = columnType.getPrimitiveJavaObject(object.get());

		if(value != null) {
			Date transformedValue = transformer.transform(value);

			writable.set(transformedValue);

			return writable;
		}

		return null;
	}
}

class HiveCharTransformerAdapter extends RangerTransformerAdapter {
	final HiveCharObjectInspector columnType;
	final HiveCharWritable        writable;

	public HiveCharTransformerAdapter(HiveCharObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer) {
		this(columnType, transformer, new HiveCharWritable());
	}

	public HiveCharTransformerAdapter(HiveCharObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer, HiveCharWritable writable) {
		super(transformer);

		this.columnType = columnType;
		this.writable   = writable;
	}

	@Override
	public Object getTransformedWritable(DeferredObject object) throws HiveException {
		HiveChar value = columnType.getPrimitiveJavaObject(object.get());

		if(value != null) {
			String transformedValue = transformer.transform(value.getValue());

			writable.set(transformedValue);

			return writable;
		}

		return null;
	}
}

class HiveVarcharTransformerAdapter extends RangerTransformerAdapter {
	final HiveVarcharObjectInspector columnType;
	final HiveVarcharWritable        writable;

	public HiveVarcharTransformerAdapter(HiveVarcharObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer) {
		this(columnType, transformer, new HiveVarcharWritable());
	}

	public HiveVarcharTransformerAdapter(HiveVarcharObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer, HiveVarcharWritable writable) {
		super(transformer);

		this.columnType = columnType;
		this.writable   = writable;
	}

	@Override
	public Object getTransformedWritable(DeferredObject object) throws HiveException {
		HiveVarchar value = columnType.getPrimitiveJavaObject(object.get());

		if(value != null) {
			String transformedValue = transformer.transform(value.getValue());

			writable.set(transformedValue);

			return writable;
		}

		return null;
	}
}

class IntegerTransformerAdapter extends RangerTransformerAdapter {
	final IntObjectInspector columnType;
	final IntWritable        writable;

	public IntegerTransformerAdapter(IntObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer) {
		this(columnType, transformer, new IntWritable());
	}

	public IntegerTransformerAdapter(IntObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer, IntWritable writable) {
		super(transformer);

		this.columnType = columnType;
		this.writable   = writable;
	}

	@Override
	public Object getTransformedWritable(DeferredObject object) throws HiveException {
		Integer value = (Integer)columnType.getPrimitiveJavaObject(object.get());

		if(value != null) {
			Integer transformedValue = transformer.transform(value);

			writable.set(transformedValue);

			return writable;
		}

		return null;
	}
}

class LongTransformerAdapter extends RangerTransformerAdapter {
	final LongObjectInspector columnType;
	final LongWritable        writable;

	public LongTransformerAdapter(LongObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer) {
		this(columnType, transformer, new LongWritable());
	}

	public LongTransformerAdapter(LongObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer, LongWritable writable) {
		super(transformer);

		this.columnType = columnType;
		this.writable   = writable;
	}

	@Override
	public Object getTransformedWritable(DeferredObject object) throws HiveException {
		Long value = (Long)columnType.getPrimitiveJavaObject(object.get());

		if(value != null) {
			Long transformedValue = transformer.transform(value);

			writable.set(transformedValue);

			return writable;
		}

		return null;
	}
}

class NoTransformAdapter extends RangerTransformerAdapter {
	final PrimitiveObjectInspector columnType;

	public NoTransformAdapter(PrimitiveObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer) {
		super(transformer);

		this.columnType = columnType;
	}

	@Override
	public Object getTransformedWritable(DeferredObject object) throws HiveException {
		return columnType.getPrimitiveWritableObject(object.get());
	}
}

class ShortTransformerAdapter extends RangerTransformerAdapter {
	final ShortObjectInspector columnType;
	final ShortWritable        writable;

	public ShortTransformerAdapter(ShortObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer) {
		this(columnType, transformer, new ShortWritable());
	}

	public ShortTransformerAdapter(ShortObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer, ShortWritable writable) {
		super(transformer);

		this.columnType = columnType;
		this.writable   = writable;
	}

	@Override
	public Object getTransformedWritable(DeferredObject object) throws HiveException {
		Short value = (Short)columnType.getPrimitiveJavaObject(object.get());

		if(value != null) {
			Short transformedValue = transformer.transform(value);

			writable.set(transformedValue);

			return writable;
		}

		return null;
	}
}

class StringTransformerAdapter extends RangerTransformerAdapter {
	final StringObjectInspector columnType;
	final Text                  writable;

	public StringTransformerAdapter(StringObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer) {
		this(columnType, transformer, new Text());
	}

	public StringTransformerAdapter(StringObjectInspector columnType, RangerBaseUdf.RangerTransformer transformer, Text writable) {
		super(transformer);

		this.columnType = columnType;
		this.writable   = writable;
	}

	@Override
	public Object getTransformedWritable(DeferredObject object) throws HiveException {
		String value = columnType.getPrimitiveJavaObject(object.get());

		if(value != null) {
			String transformedValue = transformer.transform(value);

			writable.set(transformedValue);

			return writable;
		}

		return null;
	}
}


