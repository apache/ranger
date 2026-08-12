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

package org.apache.ranger.authorization.elasticsearch.plugin.authc;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.user.User;

/**
 * Resolves the Elasticsearch-verified user for the current request.
 * Caller identity must come from X-Pack Security, not from client-supplied headers.
 */
public class ElasticsearchAuthenticatedUserResolver {
    private final SecurityContext securityContext;
    private final ThreadContext   threadContext;

    public ElasticsearchAuthenticatedUserResolver(Settings settings, ThreadContext threadContext) {
        this.securityContext = new SecurityContext(settings, threadContext);
        this.threadContext   = threadContext;
    }

    public boolean requiresAuthenticatedUser() {
        return !threadContext.isSystemContext();
    }

    public String resolveUsername() {
        String username = null;

        if (requiresAuthenticatedUser()) {
            User user = securityContext.getUser();

            if (user != null) {
                username = user.principal();
            }
        }

        return StringUtils.isEmpty(username) ? null : username;
    }
}
