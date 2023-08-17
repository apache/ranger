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

package org.apache.ranger.kms.metrics;

import java.util.concurrent.atomic.AtomicLong;

public interface KMSMetrics {

    String KMS_METRICS_CONTEXT = "kms";
    String KMS_METRIC_RECORD = "KMS";

     enum Type {
        COUNTER, GAUGE;
    }

     enum KMSMetric {

         KEY_CREATE_COUNT("KEY_CREATE_COUNT", Type.COUNTER), KEY_CREATE_ELAPSED_TIME("KEY_CREATE_ELAPSED_TIME", Type.GAUGE),
         EEK_DECRYPT_COUNT( "EEK_DECRYPT_COUNT", Type.COUNTER ), EEK_DECRYPT_ELAPSED_TIME( "EEK_DECRYPT_ELAPSED_TIME", Type.GAUGE),

         EEK_GENERATE_COUNT("EEK_GENERATE_COUNT", Type.COUNTER), EEK_GENERATE_ELAPSED_TIME("EEK_GENERATE_ELAPSED_TIME", Type.GAUGE),
         EEK_REENCRYPT_COUNT( "EEK_REENCRYPT_COUNT", Type.COUNTER), EEK_REENCRYPT_ELAPSED_TIME("EEK_REENCRYPT_ELAPSED_TIME", Type.GAUGE),

         REENCRYPT_EEK_BATCH_COUNT("REENCRYPT_EEK_BATCH_COUNT", Type.COUNTER), REENCRYPT_EEK_BATCH_ELAPSED_TIME("REENCRYPT_EEK_BATCH_ELAPSED_TIME", Type.GAUGE),
         REENCRYPT_EEK_BATCH_KEYS_COUNT("REENCRYPT_EEK_BATCH_KEYS_COUNT", Type.COUNTER),

         DELETE_KEY_COUNT("DELETE_KEY_COUNT", Type.COUNTER), DELETE_KEY_ELAPSED_TIME("DELETE_KEY_ELAPSED_TIME", Type.GAUGE),
         ROLL_NEW_VERSION_COUNT("ROLL_NEW_VERSION_COUNT", Type.COUNTER), ROLL_NEW_VERSION_ELAPSED_TIME("ROLL_NEW_VERSION_ELAPSED_TIME", Type.GAUGE),

         INVALIDATE_CACHE_COUNT("INVALIDATE_CACHE_COUNT", Type.COUNTER), INVALIDATE_CACHE_ELAPSED_TIME("INVALIDATE_CACHE_ELAPSED_TIME", Type.GAUGE),

         GET_KEYS_METADATA_COUNT("GET_KEYS_METADATA_COUNT", Type.COUNTER), GET_KEYS_METADATA_ELAPSED_TIME("GET_KEYS_METADATA_ELAPSED_TIME", Type.GAUGE),
         GET_KEYS_METADATA_KEYNAMES_COUNT("GET_KEYS_METADATA_KEYNAMES_COUNT", Type.COUNTER),

         GET_KEYS_COUNT("GET_KEYS_COUNT", Type.COUNTER), GET_KEYS_ELAPSED_TIME("GET_KEYS_ELAPSED_TIME", Type.GAUGE),
         GET_METADATA_COUNT("GET_METADATA_COUNT", Type.COUNTER), GET_METADATA_ELAPSED_TIME("GET_METADATA_ELAPSED_TIME", Type.GAUGE),

         GET_CURRENT_KEY_COUNT("GET_CURRENT_KEY_COUNT", Type.COUNTER),  GET_CURRENT_KEY_ELAPSED_TIME("GET_CURRENT_KEY_ELAPSED_TIME", Type.GAUGE),

         GET_KEY_VERSION_COUNT("GET_KEY_VERSION_COUNT", Type.COUNTER), GET_KEY_VERSION_ELAPSED_TIME("GET_KEY_VERSION_ELAPSED_TIME", Type.GAUGE),

         GET_KEY_VERSIONS_COUNT("GET_KEY_VERSIONS_COUNT", Type.COUNTER), GET_KEY_VERSIONS_ELAPSED_TIME("GET_KEY_VERSIONS_ELAPSED_TIME", Type.GAUGE),

         UNAUTHENTICATED_CALLS_COUNT("UNAUTHENTICATED_CALLS_COUNT", Type.COUNTER), UNAUTHORIZED_CALLS_COUNT("UNAUTHORIZED_CALLS_COUNT", Type.COUNTER),

         TOTAL_CALL_COUNT("TOTAL_CALL_COUNT", Type.COUNTER);

        private String key;
        private Type type;

        private AtomicLong value;

         KMSMetric(String key, Type type) {
            this.key = key;
            this.type = type;
            this.value = new AtomicLong();
        }

        public String getKey() {
            return this.key;
        }

        public Type getType(){
             return this.type;
        }

        public long getValue()
        {
            return this.value.get();
        }

        public void updateValue(long newVal)
        {
            this.value.getAndUpdate( prevVal -> prevVal + newVal);
        }

        public void incrementCounter()
        {
            this.updateValue(1);
        }


    }
}
