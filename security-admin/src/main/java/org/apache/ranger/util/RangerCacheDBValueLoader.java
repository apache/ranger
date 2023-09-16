/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.util;

import org.apache.ranger.plugin.util.RangerCache;
import org.apache.ranger.plugin.util.RangerCache.RefreshableValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public abstract class RangerCacheDBValueLoader<K, V> extends RangerCache.ValueLoader<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(RangerCacheDBValueLoader.class);

    private final TransactionTemplate txTemplate;

    public RangerCacheDBValueLoader(PlatformTransactionManager txManager) {
        this.txTemplate = new TransactionTemplate(txManager);

        txTemplate.setReadOnly(true);
    }

    @Override
    public RefreshableValue<V> load(K key, RefreshableValue<V> currentValue) throws Exception {
        Exception[] ex = new Exception[1];

        RefreshableValue<V> ret = txTemplate.execute(status -> {
            try {
                return dbLoad(key, currentValue);
            } catch (Exception excp) {
                LOG.error("RangerDBLoaderCache.load(): failed to load for key={}", key, excp);

                ex[0] = excp;
            }

            return null;
        });

        if (ex[0] != null) {
            throw ex[0];
        }

        return ret;
    }

    protected abstract RefreshableValue<V> dbLoad(K key, RefreshableValue<V> currentValue) throws Exception;
}
