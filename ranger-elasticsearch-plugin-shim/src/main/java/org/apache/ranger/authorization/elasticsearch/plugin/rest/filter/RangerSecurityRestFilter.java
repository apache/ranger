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

package org.apache.ranger.authorization.elasticsearch.plugin.rest.filter;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authorization.elasticsearch.plugin.authc.ElasticsearchAuthenticatedUserResolver;
import org.apache.ranger.authorization.elasticsearch.plugin.authc.user.UsernamePasswordToken;
import org.apache.ranger.authorization.elasticsearch.plugin.utils.RequestUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerSecurityRestFilter extends AbstractLifecycleComponent implements RestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RangerSecurityRestFilter.class);

    private final Settings      settings;
    private final RestHandler   restHandler;
    private final ThreadContext threadContext;

    public RangerSecurityRestFilter(final Settings settings, final ThreadContext threadContext, final RestHandler restHandler) {
        super();

        this.settings      = settings;
        this.restHandler   = restHandler;
        this.threadContext = threadContext;
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final NodeClient client) throws Exception {
        ElasticsearchAuthenticatedUserResolver authResolver = new ElasticsearchAuthenticatedUserResolver(settings, threadContext);

        if (authResolver.requiresAuthenticatedUser()) {
            String username = authResolver.resolveUsername();

            if (StringUtils.isEmpty(username)) {
                throw new ElasticsearchStatusException("Error: Request requires authenticated user.", RestStatus.UNAUTHORIZED);
            }

            threadContext.putTransient(UsernamePasswordToken.USERNAME, username);
            LOG.debug("Using Elasticsearch-verified user[{}] for request[{}].", username, request);
        }

        String clientIPAddress = RequestUtils.getClientIPAddress(request);

        if (StringUtils.isNotEmpty(clientIPAddress)) {
            threadContext.putTransient(RequestUtils.CLIENT_IP_ADDRESS, clientIPAddress);
        }

        this.restHandler.handleRequest(request, channel, client);
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
    }
}
