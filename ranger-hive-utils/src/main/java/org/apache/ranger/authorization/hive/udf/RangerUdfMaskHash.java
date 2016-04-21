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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;


public class RangerUdfMaskHash extends RangerBaseUdf {
    public RangerUdfMaskHash() {
        super(new MaskHashTransformer());
    }
}

class MaskHashTransformer extends RangerBaseUdf.RangerTransformer {
    @Override
    public void init(ObjectInspector[] arguments, int startIdx) {
    }

    @Override
    String transform(String value) {
        return DigestUtils.md5Hex(value);
    }

    @Override
    Byte transform(Byte value) {
        return value;
    }

    @Override
    Short transform(Short value) {
        return value;
    }

    @Override
    Integer transform(Integer value) {
        return value;
    }

    @Override
    Long transform(Long value) {
        return value;
    }

    @Override
    Date transform(Date value) {
        return value;
    }
}
