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

package org.apache.ranger.authorization.elasticsearch.plugin;

import org.apache.ranger.authorization.elasticsearch.authorizer.RangerElasticsearchAuthorizerDelegate;
import org.apache.ranger.authorization.elasticsearch.plugin.action.filter.RangerSecurityActionFilter;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Ranger authorization integrates with X-Pack Security: authentication and REST handling
 * are delegated to x-pack-security; Ranger enforces policies via {@link RangerSecurityActionFilter}
 * using the verified user from {@link org.elasticsearch.xpack.core.security.SecurityContext}.
 */
public class RangerElasticsearchPlugin extends Plugin implements ActionPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(RangerElasticsearchPlugin.class);

    private static final String RANGER_ELASTICSEARCH_PLUGIN_CONF_NAME = "ranger-elasticsearch-plugin";

    private final Settings settings;

    private RangerSecurityActionFilter rangerSecurityActionFilter;

    public RangerElasticsearchPlugin(Settings settings) {
        this.settings = settings;

        LOG.debug("settings:{}", this.settings);
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return Collections.singletonList(rangerSecurityActionFilter);
    }

    @Override
    public Collection<Object> createComponents(final Client client, final ClusterService clusterService, final ThreadPool threadPool, final ResourceWatcherService resourceWatcherService,
            final ScriptService scriptService, final NamedXContentRegistry xContentRegistry, final Environment environment, final NodeEnvironment nodeEnvironment,
            final NamedWriteableRegistry namedWriteableRegistry, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<RepositoriesService> repositoriesServiceSupplier) {
        Path configPath = registerPluginConfigDir(environment);

        ThreadContext threadContext = threadPool.getThreadContext();

        RangerElasticsearchAuthorizerDelegate authorizer = initAuthorizer(threadContext, configPath);

        rangerSecurityActionFilter = new RangerSecurityActionFilter(settings, threadContext, authorizer);

        return Collections.singletonList(rangerSecurityActionFilter);
    }

    /**
     * Initialize Ranger in Elasticsearch system context so Hadoop/Ranger plugin setup is not
     * blocked by the plugin security manager during node startup.
     */
    private RangerElasticsearchAuthorizerDelegate initAuthorizer(ThreadContext threadContext, Path configPath) {
        String configDir = configPath != null ? configPath.toAbsolutePath().toString() : null;

        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.markAsSystemContext();

            return new RangerElasticsearchAuthorizerDelegate(configDir);
        }
    }

    /**
     * Resolve the on-disk Ranger config directory for the authorizer implementation.
     * ES 7.17 on Java 17 cannot extend the plugin classloader via reflection.
     */
    private Path registerPluginConfigDir(Environment environment) {
        Path configPath = environment.configFile().resolve(RANGER_ELASTICSEARCH_PLUGIN_CONF_NAME);

        if (!Files.isDirectory(configPath)) {
            LOG.error("Ranger elasticsearch plugin config directory [{}] does not exist.", configPath);

            return null;
        }

        LOG.info("Using Ranger elasticsearch plugin config directory [{}].", configPath);

        return configPath;
    }
}
