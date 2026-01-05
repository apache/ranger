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

package org.apache.ranger.credentialapi;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;

import java.util.ArrayList;
import java.util.List;

public class CredentialReader {
    private CredentialReader() {
        // to block instantiation
    }

    public static String getDecryptedString(String crendentialProviderPath, String alias, String storeType) {
        String credential = null;
        try {
            if (crendentialProviderPath == null || alias == null) {
                return null;
            }
            char[]        pass                                = null;
            Configuration conf                                = new Configuration();
            String        crendentialProviderPrefixJceks      = JavaKeyStoreProvider.SCHEME_NAME + "://file";
            String        crendentialProviderPrefixLocalJceks = "localjceks://file";
            crendentialProviderPrefixJceks = crendentialProviderPrefixJceks.toLowerCase();

            String crendentialProviderPrefixBcfks      = "bcfks" + "://file";
            String crendentialProviderPrefixLocalBcfks = "localbcfks" + "://file";
            crendentialProviderPrefixBcfks      = crendentialProviderPrefixBcfks.toLowerCase();
            crendentialProviderPrefixLocalBcfks = crendentialProviderPrefixLocalBcfks.toLowerCase();

            crendentialProviderPath = crendentialProviderPath.trim();
            alias                   = alias.trim();
            if (crendentialProviderPath.toLowerCase().startsWith(crendentialProviderPrefixJceks) || crendentialProviderPath.toLowerCase().startsWith(crendentialProviderPrefixLocalJceks) || crendentialProviderPath.toLowerCase().startsWith(crendentialProviderPrefixBcfks) || crendentialProviderPath.toLowerCase().startsWith(crendentialProviderPrefixLocalBcfks)) {
                conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, crendentialProviderPath);
            } else {
                if (crendentialProviderPath.startsWith("/")) {
                    if (StringUtils.equalsIgnoreCase(storeType, "bcfks")) {
                        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, crendentialProviderPath);
                    } else {
                        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, JavaKeyStoreProvider.SCHEME_NAME + "://file" + crendentialProviderPath);
                    }
                } else {
                    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, JavaKeyStoreProvider.SCHEME_NAME + "://file/" + crendentialProviderPath);
                }
            }
            List<CredentialProvider>           providers   = CredentialProviderFactory.getProviders(conf);
            List<String>                       aliasesList = new ArrayList<String>();
            CredentialProvider.CredentialEntry credEntry   = null;
            for (CredentialProvider provider : providers) {
                //System.out.println("Credential Provider :" + provider);
                aliasesList = provider.getAliases();
                if (aliasesList != null && aliasesList.contains(alias.toLowerCase())) {
                    credEntry = null;
                    credEntry = provider.getCredentialEntry(alias.toLowerCase());
                    pass      = credEntry.getCredential();
                    if (pass != null && pass.length > 0) {
                        credential = String.valueOf(pass);
                        break;
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            credential = null;
        }
        return credential;
    }
}
