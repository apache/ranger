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

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestRangerSecurityRestFilter {
    @Test
    void rejectsUnverifiedBasicCredentials() {
        Settings      settings      = Settings.builder().build();
        ThreadContext threadContext = new ThreadContext(settings);
        RestHandler   restHandler   = Mockito.mock(RestHandler.class);
        RestRequest   request       = Mockito.mock(RestRequest.class);
        RestChannel   channel       = Mockito.mock(RestChannel.class);
        NodeClient    client        = Mockito.mock(NodeClient.class);
        RangerSecurityRestFilter filter = new RangerSecurityRestFilter(settings, threadContext, restHandler);

        Mockito.when(request.getHeaders()).thenReturn(java.util.Collections.singletonMap(
                "Authorization", java.util.Collections.singletonList("Basic YWRtaW46d3JvbmdwYXNzd29yZA==")));

        ElasticsearchStatusException exception = Assertions.assertThrows(
                ElasticsearchStatusException.class,
                () -> filter.handleRequest(request, channel, client));

        Assertions.assertEquals(RestStatus.UNAUTHORIZED, exception.status());
        Mockito.verifyNoInteractions(restHandler);
    }

    @Test
    void acceptsElasticsearchVerifiedUser() throws Exception {
        Settings        settings        = Settings.builder().build();
        ThreadContext   threadContext   = new ThreadContext(settings);
        SecurityContext securityContext = new SecurityContext(settings, threadContext);
        RestHandler     restHandler     = Mockito.mock(RestHandler.class);
        RestRequest     request         = Mockito.mock(RestRequest.class);
        RestChannel     channel         = Mockito.mock(RestChannel.class);
        NodeClient      client          = Mockito.mock(NodeClient.class);
        HttpChannel     httpChannel     = Mockito.mock(HttpChannel.class);
        RangerSecurityRestFilter filter = new RangerSecurityRestFilter(settings, threadContext, restHandler);

        Mockito.when(request.getHttpChannel()).thenReturn(httpChannel);
        Mockito.when(httpChannel.getRemoteAddress()).thenReturn(null);

        securityContext.setUser(new User("admin"), Version.CURRENT);

        filter.handleRequest(request, channel, client);

        Mockito.verify(restHandler).handleRequest(request, channel, client);
    }
}
