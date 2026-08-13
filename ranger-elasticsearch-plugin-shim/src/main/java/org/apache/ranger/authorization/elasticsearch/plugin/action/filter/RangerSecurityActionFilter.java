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

package org.apache.ranger.authorization.elasticsearch.plugin.action.filter;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authorization.elasticsearch.authorizer.RangerElasticsearchAuthorizerDelegate;
import org.apache.ranger.authorization.elasticsearch.plugin.authc.ElasticsearchAuthenticatedUserResolver;
import org.apache.ranger.authorization.elasticsearch.plugin.authc.user.UsernamePasswordToken;
import org.apache.ranger.authorization.elasticsearch.plugin.utils.RequestUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;

import java.util.List;

public class RangerSecurityActionFilter extends AbstractLifecycleComponent implements ActionFilter {
    private final Settings                      settings;
    private final ThreadContext                 threadContext;
    private final RangerElasticsearchAuthorizerDelegate rangerElasticsearchAuthorizer;

    public RangerSecurityActionFilter(Settings settings, ThreadContext threadContext, RangerElasticsearchAuthorizerDelegate rangerElasticsearchAuthorizer) {
        super();

        this.settings                      = settings;
        this.threadContext                 = threadContext;
        this.rangerElasticsearchAuthorizer = rangerElasticsearchAuthorizer;
    }

    /**
     * Run after x-pack {@code SecurityActionFilter} (order {@code Integer.MIN_VALUE}) so
     * {@link org.elasticsearch.xpack.core.security.SecurityContext} holds the verified user.
     */
    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, String action, Request request, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
        String user = threadContext.getTransient(UsernamePasswordToken.USERNAME);

        ElasticsearchAuthenticatedUserResolver authResolver = new ElasticsearchAuthenticatedUserResolver(settings, threadContext);
        List<String>                           groups       = null;

        if (StringUtils.isEmpty(user)) {
            if (authResolver.requiresAuthenticatedUser()) {
                user = authResolver.resolveUsername();

                if (StringUtils.isNotEmpty(user)) {
                    threadContext.putTransient(UsernamePasswordToken.USERNAME, user);
                }
            }
        }

        if (StringUtils.isNotEmpty(user)) {
            List<String> roles = authResolver.resolveRoles();

            if (!roles.isEmpty()) {
                groups = roles;
            }
        }

        if (shouldBypassRangerCheck(user, action)) {
            chain.proceed(task, action, request, listener);
            return;
        }

        if (StringUtils.isNotEmpty(user)) {
            List<String> indexs          = RequestUtils.getIndexFromRequest(request);
            String       clientIPAddress = threadContext.getTransient(RequestUtils.CLIENT_IP_ADDRESS);

            for (String index : indexs) {
                boolean result = rangerElasticsearchAuthorizer.checkPermission(user, groups, index, action, clientIPAddress);

                if (!result) {
                    String errorMsg = "Error: User[{}] could not do action[{}] on index[{}]";

                    throw new ElasticsearchStatusException(errorMsg, RestStatus.FORBIDDEN, user, action, index);
                }
            }
        } else {
            throw new ElasticsearchStatusException("Error: Request requires authenticated user.", RestStatus.UNAUTHORIZED);
        }

        chain.proceed(task, action, request, listener);
    }

    private boolean shouldBypassRangerCheck(String user, String action) {
        if (threadContext.isSystemContext()) {
            return true;
        }

        if (StringUtils.isNotEmpty(user) && user.charAt(0) == '_') {
            return true;
        }

        return StringUtils.isNotEmpty(action) && action.startsWith("internal:");
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
