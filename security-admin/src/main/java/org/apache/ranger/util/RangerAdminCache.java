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

package org.apache.ranger.util;

import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.plugin.util.RangerCache;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.security.context.RangerSecurityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public class RangerAdminCache<K, V> extends RangerCache<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(RangerDBValueLoader.class);

    public static final int         DEFAULT_ADMIN_CACHE_LOADER_THREADS_COUNT     = 1;
    public static final RefreshMode DEFAULT_ADMIN_CACHE_REFRESH_MODE             = RefreshMode.ON_ACCESS;
    public static final long        DEFAULT_ADMIN_CACHE_VALUE_VALIDITY_PERIOD_MS = 0;          // every access should look to refresh
    public static final long        DEFAULT_ADMIN_CACHE_VALUE_INIT_TIMEOUT_MS    = -1L;        // infinite timeout
    public static final long        DEFAULT_ADMIN_CACHE_VALUE_REFRESH_TIMEOUT_MS = 10 * 1000L; // 10 seconds

    private static final String PROP_PREFIX                   = "ranger.admin.cache.";
    private static final String PROP_LOADER_THREAD_POOL_SIZE  = ".loader.threadpool.size";
    private static final String PROP_VALUE_INIT_TIMEOUT_MS    = ".value.init.timeout.ms";
    private static final String PROP_VALUE_REFRESH_TIMEOUT_MS = ".value.refresh.timeout.ms";

    protected RangerAdminCache(String name, RangerDBValueLoader<K, V> loader) {
        this(name, loader, getLoaderThreadPoolSize(name), DEFAULT_ADMIN_CACHE_REFRESH_MODE, DEFAULT_ADMIN_CACHE_VALUE_VALIDITY_PERIOD_MS, getValueInitLoadTimeout(name), getValueRefreshLoadTimeout(name));
    }

    protected RangerAdminCache(String name, RangerDBValueLoader<K, V> loader, int loaderThreadsCount, RefreshMode refreshMode, long valueValidityPeriodMs, long valueInitLoadTimeoutMs, long valueRefreshLoadTimeoutMs) {
        super(name, loader, loaderThreadsCount, refreshMode, valueValidityPeriodMs, valueInitLoadTimeoutMs, valueRefreshLoadTimeoutMs);
    }

    @Override
    public V get(K key)  {
        return super.get(key, RangerContextHolder.getSecurityContext());
    }

    private static int getLoaderThreadPoolSize(String cacheName) {
        return RangerAdminConfig.getInstance().getInt(PROP_PREFIX + cacheName + PROP_LOADER_THREAD_POOL_SIZE, DEFAULT_ADMIN_CACHE_LOADER_THREADS_COUNT);
    }

    private static long getValueInitLoadTimeout(String cacheName) {
        return RangerAdminConfig.getInstance().getLong(PROP_PREFIX + cacheName + PROP_VALUE_INIT_TIMEOUT_MS, DEFAULT_ADMIN_CACHE_VALUE_INIT_TIMEOUT_MS);
    }

    private static long getValueRefreshLoadTimeout(String cacheName) {
        return RangerAdminConfig.getInstance().getLong(PROP_PREFIX + cacheName + PROP_VALUE_REFRESH_TIMEOUT_MS, DEFAULT_ADMIN_CACHE_VALUE_REFRESH_TIMEOUT_MS);
    }

    public abstract static class RangerDBValueLoader<K, V> extends ValueLoader<K, V> {
        private final TransactionTemplate txTemplate;

        public RangerDBValueLoader(PlatformTransactionManager txManager) {
            this.txTemplate = new TransactionTemplate(txManager);

            txTemplate.setReadOnly(true);
        }

        @Override
        final public RefreshableValue<V> load(K key, RefreshableValue<V> currentValue, Object context) throws Exception {
            Exception[] ex = new Exception[1];

            RefreshableValue<V> ret = txTemplate.execute(status -> {
                RangerSecurityContext currentContext = null;

                try {
                    if (context instanceof RangerSecurityContext) {
                        currentContext = RangerContextHolder.getSecurityContext();

                        RangerContextHolder.setSecurityContext((RangerSecurityContext) context);
                    }

                    return dbLoad(key, currentValue);
                } catch (Exception excp) {
                    LOG.error("RangerDBLoaderCache.load(): failed to load for key={}", key, excp);

                    ex[0] = excp;
                } finally {
                    if (context instanceof RangerSecurityContext) {
                        RangerContextHolder.setSecurityContext(currentContext);
                    }
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
}
