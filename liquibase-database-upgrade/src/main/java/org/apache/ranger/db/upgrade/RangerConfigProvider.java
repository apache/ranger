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
package org.apache.ranger.db.upgrade;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.credentialapi.CredentialReader;
import org.apache.ranger.server.tomcat.EmbeddedServer;
import org.apache.ranger.server.tomcat.EmbeddedServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Component("ranger")
@Lazy
public class RangerConfigProvider implements IConfigProvider {
    private static final Logger LOG = LoggerFactory.getLogger(RangerConfigProvider.class);

    @Override
    public String getUrl() {
        return EmbeddedServerUtil.getConfig("ranger.jpa.jdbc.url");
    }

    @Override
    public String getUsername() {
        return EmbeddedServerUtil.getConfig("ranger.jpa.jdbc.user");
    }

    @Override
    public String getPassword() {
        LOG.info("==> getPassword()");
        String providerPath = EmbeddedServerUtil.getConfig("ranger.credential.provider.path");
        String keyAlias     = EmbeddedServerUtil.getConfig("ranger.jpa.jdbc.credential.alias", "keyStoreCredentialAlias");
        String storeType    = EmbeddedServerUtil.getConfig("ranger.keystore.file.type", EmbeddedServer.RANGER_KEYSTORE_FILE_TYPE_DEFAULT);
        String keystorePass = null;
        if (providerPath != null && keyAlias != null) {
            keystorePass = CredentialReader.getDecryptedString(providerPath.trim(), keyAlias.trim(), storeType.trim());
            if (StringUtils.isBlank(keystorePass) || "none".equalsIgnoreCase(keystorePass.trim())) {
                keystorePass = EmbeddedServerUtil.getConfig("ranger.service.https.attrib.keystore.pass");
            }
        }
        LOG.info("<== getPassword()");
        return keystorePass;
    }

    @Override
    public String getDriver() {
        return EmbeddedServerUtil.getConfig("ranger.jpa.jdbc.driver");
    }

    @Override
    public String getMasterChangelogRelativePath() {
        return Constants.RANGER_UPGRADE_CHANGELOG_RELATIVE_PATH;
    }

    @Override
    public String getFinalizeChangelogRelativePath() {
        return Constants.RANGER_FINALIZE_CHANGELOG_RELATIVE_PATH;
    }
}
