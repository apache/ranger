/*
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

package org.apache.ranger.entraid.graph;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Form;

import java.util.Arrays;

final class ClientSecretTokenProvider extends OAuthTokenProvider {
    private char[] clientSecret;

    @Override
    public void init(EntraIdGraphConfig config, Client httpClient) throws GraphClientException {
        super.init(config, httpClient);
        char[] secret = config.getClientSecret();
        if (secret == null || secret.length == 0) {
            throw new GraphClientException("clientSecret is required for CLIENT_SECRET authentication");
        }
        this.clientSecret = secret;
    }

    @Override
    protected void addCredential(Form form) {
        form.param("client_secret", new String(clientSecret));
    }

    void destroy() {
        if (clientSecret != null) {
            Arrays.fill(clientSecret, '\0');
            clientSecret = null;
        }
    }
}
