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

package org.apache.ranger.authorization.hive.udf;


import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class RangerTransformerFactory {
    private static final Log LOG = LogFactory.getLog(RangerTransformerFactory.class);


    public static RangerTransformer getTransformer(String transformType, String transformerImpl, String initParam) {
        RangerTransformer ret = null;

        if(StringUtils.isEmpty(transformerImpl)) {
            LOG.warn("maskType: '" + transformType + "': no transformer specified. Will use '" + MaskTransformer.class.getName() + "' as transformer");

            ret = new MaskTransformer();
        } else {
            ret = createTransformer(transformerImpl);

            if(ret == null) {
                LOG.warn("maskType: '" + transformType + "': failed to create transformer '" + transformerImpl + "'. Will use '" + MaskTransformer.class.getName() + "' as transformer");

                ret = new MaskTransformer();
            }
        }

        if(initParam == null) {
            initParam = "";
        }

        ret.init(initParam);

        return ret;
    }

	private static RangerTransformer createTransformer(String className) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTransformerFactory.createTransformer(" + className + ")");
		}

		RangerTransformer ret = null;

		try {
			@SuppressWarnings("unchecked")
			Class<RangerTransformer> transformerClass = (Class<RangerTransformer>)Class.forName(className);

            ret = transformerClass.newInstance();
		} catch(Throwable t) {
			LOG.error("RangerTransformerFactory.createTransformer(" + className + "): error instantiating transformer", t);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTransformerFactory.createTransformer(" + className + "): " + ret);
		}

		return ret;
	}
}
