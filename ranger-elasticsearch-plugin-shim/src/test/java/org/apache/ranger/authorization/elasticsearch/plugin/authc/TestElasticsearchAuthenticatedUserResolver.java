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

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestElasticsearchAuthenticatedUserResolver {
    @Test
    void resolveUsernameReturnsVerifiedUser() {
        Settings      settings      = Settings.builder().build();
        ThreadContext threadContext = new ThreadContext(settings);
        SecurityContext securityContext = new SecurityContext(settings, threadContext);

        securityContext.setUser(new User("admin"), Version.CURRENT);

        ElasticsearchAuthenticatedUserResolver resolver = new ElasticsearchAuthenticatedUserResolver(settings, threadContext);

        Assertions.assertTrue(resolver.requiresAuthenticatedUser());
        Assertions.assertEquals("admin", resolver.resolveUsername());
    }

    @Test
    void resolveUsernameReturnsNullWithoutVerifiedUser() {
        Settings      settings      = Settings.builder().build();
        ThreadContext threadContext = new ThreadContext(settings);
        ElasticsearchAuthenticatedUserResolver resolver = new ElasticsearchAuthenticatedUserResolver(settings, threadContext);

        Assertions.assertTrue(resolver.requiresAuthenticatedUser());
        Assertions.assertNull(resolver.resolveUsername());
    }

    @Test
    void systemContextDoesNotRequireAuthenticatedUser() {
        Settings      settings      = Settings.builder().build();
        ThreadContext threadContext = new ThreadContext(settings);

        threadContext.markAsSystemContext();

        ElasticsearchAuthenticatedUserResolver resolver = new ElasticsearchAuthenticatedUserResolver(settings, threadContext);

        Assertions.assertFalse(resolver.requiresAuthenticatedUser());
        Assertions.assertNull(resolver.resolveUsername());
    }
}
