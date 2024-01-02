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

package org.apache.ranger.plugin.util;

import com.sun.istack.NotNull;
import org.apache.ranger.plugin.util.AutoClosableLock.AutoClosableTryLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;


public class RangerCache<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(RangerCache.class);

    public enum RefreshMode { ON_ACCESS, ON_SCHEDULE } // when to refresh the value: when a value is accessed? or on a scheduled interval?

    private static final AtomicInteger CACHE_NUMBER                     = new AtomicInteger(1);
    private static final String        CACHE_LOADER_THREAD_PREFIX       = "ranger-cache-";
    private static final int           DEFAULT_LOADER_THREADS_COUNT     = 10;
    private static final RefreshMode   DEFAULT_REFRESH_MODE             = RefreshMode.ON_ACCESS;
    private static final int           DEFAULT_VALUE_VALIDITY_PERIOD_MS = 30 * 1000;
    private static final int           DEFAULT_VALUE_INIT_TIMEOUT_MS    = -1; // infinite timeout
    private static final int           DEFAULT_VALUE_REFRESH_TIMEOUT_MS = 10;

    private final String              name;
    private final Map<K, CachedValue> cache;
    private       ValueLoader<K, V>   loader;                    // loader implementation that fetches the value for a given key from the source
    private final int                 loaderThreadsCount;        // number of threads to use for loading values into cache
    private final RefreshMode         refreshMode;               // when to refresh a cached value: when a value is accessed? or on a scheduled interval?
    private final long                valueValidityPeriodMs;     // minimum interval before a cached value is refreshed
    private final long                valueInitLoadTimeoutMs;    // max time a caller would wait if cache doesn't have the value
    private final long                valueRefreshLoadTimeoutMs; // max time a caller would wait if cache already has the value, but needs refresh
    private final ExecutorService     loaderThreadPool;

    protected RangerCache(String name, ValueLoader<K, V> loader) {
        this(name, loader, DEFAULT_LOADER_THREADS_COUNT, DEFAULT_REFRESH_MODE, DEFAULT_VALUE_VALIDITY_PERIOD_MS, DEFAULT_VALUE_INIT_TIMEOUT_MS, DEFAULT_VALUE_REFRESH_TIMEOUT_MS);
    }

    protected RangerCache(String name, ValueLoader<K, V> loader, int loaderThreadsCount, RefreshMode refreshMode, long valueValidityPeriodMs, long valueInitLoadTimeoutMs, long valueRefreshLoadTimeoutMs) {
        this.name                      = name;
        this.cache                     = new ConcurrentHashMap<>();
        this.loader                    = loader;
        this.loaderThreadsCount        = loaderThreadsCount;
        this.refreshMode               = refreshMode;
        this.valueValidityPeriodMs     = valueValidityPeriodMs;
        this.valueInitLoadTimeoutMs    = valueInitLoadTimeoutMs;
        this.valueRefreshLoadTimeoutMs = valueRefreshLoadTimeoutMs;

        if (this.refreshMode == RefreshMode.ON_SCHEDULE) {
            this.loaderThreadPool = Executors.newScheduledThreadPool(loaderThreadsCount, createThreadFactory());
        } else {
            this.loaderThreadPool = Executors.newFixedThreadPool(loaderThreadsCount, createThreadFactory());
        }

        LOG.info("Created RangerCache(name={}): loaderThreadsCount={}, refreshMode={}, valueValidityPeriodMs={}, valueInitLoadTimeoutMs={}, valueRefreshLoadTimeoutMs={}", name, loaderThreadsCount, refreshMode, valueValidityPeriodMs, valueInitLoadTimeoutMs, valueRefreshLoadTimeoutMs);
    }

    protected void setLoader(ValueLoader<K, V> loader) { this.loader = loader; }

    public String getName() { return name; }

    public ValueLoader<K, V> getLoader() { return loader; }

    public int getLoaderThreadsCount() { return loaderThreadsCount; }

    public RefreshMode getRefreshMode() { return refreshMode; }

    public long getValueValidityPeriodMs() { return valueValidityPeriodMs; }

    public long getValueInitLoadTimeoutMs() { return valueInitLoadTimeoutMs; }

    public long getValueRefreshLoadTimeoutMs() { return valueRefreshLoadTimeoutMs; }

    public V get(K key) {
        return get(key, null);
    }

    public Set<K> getKeys() {
        return new HashSet<>(cache.keySet());
    }

    public void addIfAbsent(K key) {
        cache.computeIfAbsent(key, f -> new CachedValue(key));
    }

    public V remove(K key) {
        CachedValue value = cache.remove(key);
        final V     ret;

        if (value != null) {
            value.isRemoved = true; // so that the refresher thread doesn't schedule next refresh

            ret = value.getCurrentValue();
        } else {
            ret = null;
        }

        return ret;
    }

    public boolean isLoaded(K key) {
        CachedValue         entry = cache.get(key);
        RefreshableValue<V> value = entry != null ? entry.value : null;

        return value != null;
    }

    protected V get(K key, Object context) {
        final long        startTime = System.currentTimeMillis();
        final CachedValue value     = cache.computeIfAbsent(key, f -> new CachedValue(key));
        final long        timeoutMs = value.isInitialized() ? valueRefreshLoadTimeoutMs : valueInitLoadTimeoutMs;
        final V           ret;

        if (timeoutMs >= 0) {
            final long timeTaken = System.currentTimeMillis() - startTime;

            if (timeoutMs <= timeTaken) {
                ret = value.getCurrentValue();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("key={}: cache-lookup={}ms took longer than timeout={}ms. Using current value {}", key, timeTaken, timeoutMs, ret);
                }
            } else {
                ret = value.getValue(timeoutMs - timeTaken);
            }
        } else {
            ret = value.getValue(context);
        }

        return ret;
    }

    public static class RefreshableValue<V> {
        private final V    value;
        private       long nextRefreshTimeMs = -1;

        public RefreshableValue(V value) {
            this.value = value;
        }

        public V getValue() { return value; }

        public boolean needsRefresh() {
            return nextRefreshTimeMs == -1 || System.currentTimeMillis() > nextRefreshTimeMs;
        }

        private void setNextRefreshTimeMs(long nextRefreshTimeMs) {this.nextRefreshTimeMs = nextRefreshTimeMs; }
    }

    public static abstract class ValueLoader<K, V> {
        public abstract RefreshableValue<V> load(K key, RefreshableValue<V> currentValue, Object context) throws Exception;
    }

    private class CachedValue {
        private final    ReentrantLock       lock = new ReentrantLock();
        private final    K                   key;
        private volatile boolean             isRemoved = false;
        private volatile RefreshableValue<V> value     = null;
        private volatile Future<?>           refresher = null;

        private CachedValue(K key) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("CachedValue({})", key);
            }

            this.key = key;
        }

        public K getKey() { return key; }

        public V getValue(Object context) {
            refreshIfNeeded(context);

            return getCurrentValue();
        }

        public V getValue(long timeoutMs, Object context) {
            if (timeoutMs < 0) {
                refreshIfNeeded(context);
            } else {
                refreshIfNeeded(timeoutMs, context);
            }

            return getCurrentValue();
        }

        public V getCurrentValue() {
            RefreshableValue<V> value = this.value;

            return value != null ? value.getValue() : null;
        }

        public boolean needsRefresh() {
            return !isInitialized() || (refreshMode == RefreshMode.ON_ACCESS && value.needsRefresh());
        }

        public boolean isInitialized() {
            RefreshableValue<V> value = this.value;

            return value != null;
        }

        private void refreshIfNeeded(Object context) {
            if (needsRefresh()) {
                try (AutoClosableLock ignored = new AutoClosableLock(lock)) {
                    if (needsRefresh()) {
                        Future<?> future = this.refresher;

                        if (future == null) { // refresh from current thread
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("refreshIfNeeded(key={}): using caller thread", key);
                            }

                            refreshValue(context);
                        } else { // wait for the refresher to complete
                            try {
                                future.get();

                                this.refresher = null;
                            } catch (InterruptedException | ExecutionException excp) {
                                LOG.warn("refreshIfNeeded(key={}) failed", key, excp);
                            }
                        }
                    }
                }
            }
        }

        private void refreshIfNeeded(long timeoutMs, Object context) {
            if (needsRefresh()) {
                long startTime = System.currentTimeMillis();

                try (AutoClosableTryLock tryLock = new AutoClosableTryLock(lock, timeoutMs, TimeUnit.MILLISECONDS)) {
                    if (tryLock.isLocked()) {
                        if (needsRefresh()) {
                            Future<?> future = this.refresher;

                            if (future == null) {
                                future = this.refresher = loaderThreadPool.submit(new RefreshWithContext(context));

                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("refresher scheduled for key {}", key);
                                }
                            } else {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("refresher already exists for key {}", key);
                                }
                            }

                            long timeLeftMs = timeoutMs - (System.currentTimeMillis() - startTime);

                            if (timeLeftMs > 0) {
                                try {
                                    future.get(timeLeftMs, TimeUnit.MILLISECONDS);

                                    this.refresher = null;
                                } catch (TimeoutException | InterruptedException | ExecutionException excp) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("refreshIfNeeded(key={}, timeoutMs={}) failed", key, timeoutMs, excp);
                                    }
                                }
                            }
                        }
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("refreshIfNeeded(key={}, timeoutMs={}) couldn't obtain lock", key, timeoutMs);
                        }
                    }
                }
            }
        }

        private Boolean refreshValue(Object context) {
            long                startTime = System.currentTimeMillis();
            boolean             isSuccess = false;
            RefreshableValue<V> newValue  = null;

            try {
                ValueLoader<K, V> loader = RangerCache.this.loader;

                if (loader != null) {
                    newValue  = loader.load(key, value, context);
                    isSuccess = true;
                }
            } catch (KeyNotFoundException excp) {
                LOG.debug("refreshValue(key={}) failed with KeyNotFoundException. Removing it", key, excp);

                remove(key); // remove the key from cache (so that next get() will try to load it again
            } catch (Exception excp) {
                LOG.warn("refreshValue(key={}) failed", key, excp);

                // retain the old value, update the loadTime
                newValue = value;
            } finally {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("refresher {} for key {}, timeTaken={}", (isSuccess ? "completed" : "failed"), key, (System.currentTimeMillis() - startTime));
                }

                setValue(newValue);

                if (refreshMode == RefreshMode.ON_SCHEDULE) {
                     if (!isRemoved) {
                         ScheduledExecutorService scheduledExecutor = ((ScheduledExecutorService) loaderThreadPool);

                         scheduledExecutor.schedule(new RefreshWithContext(context), valueValidityPeriodMs, TimeUnit.MILLISECONDS);
                     } else {
                         if (LOG.isDebugEnabled()) {
                             LOG.debug("key {} was removed. Not scheduling next refresh ", key);
                         }
                     }
                }
            }

            return Boolean.TRUE;
        }

        private void setValue(RefreshableValue<V> value) {
            if (value != null) {
                this.value = value;

                this.value.setNextRefreshTimeMs(System.currentTimeMillis() + valueValidityPeriodMs);
            }
        }

        private class RefreshWithContext implements Callable<Boolean> {
            private final Object context;

            public RefreshWithContext(Object context) {
                this.context = context;
            }

            @Override
            public Boolean call() {
                return refreshValue(context);
            }
        }
    }

    private ThreadFactory createThreadFactory() {
        return new ThreadFactory() {
            private final String        namePrefix = CACHE_LOADER_THREAD_PREFIX + CACHE_NUMBER.getAndIncrement() + "-" + name;
            private final AtomicInteger number     = new AtomicInteger(1);

            @Override
            public Thread newThread(@NotNull Runnable r) {
                Thread t = new Thread(r, namePrefix + number.getAndIncrement());

                if (!t.isDaemon()) {
                    t.setDaemon(true);
                }

                if (t.getPriority() != Thread.NORM_PRIORITY) {
                    t.setPriority(Thread.NORM_PRIORITY);
                }

                return t;
            }
        };
    }

    public static class KeyNotFoundException extends Exception {
        public KeyNotFoundException(String msg) {
            super(msg);
        }
    }
}
