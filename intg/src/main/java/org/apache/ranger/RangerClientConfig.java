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

package org.apache.ranger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

public class RangerClientConfig  {

    private static final Logger LOG = LoggerFactory.getLogger(RangerClientConfig.class);

    private static final String RANGER_ADMIN_URL          = "ranger.client.url";
    private static final String AUTH_TYPE                 = "ranger.client.authentication.type";
    private static final String CLIENT_KERBEROS_PRINCIPAL = "ranger.client.kerberos.principal";
    private static final String CLIENT_KERBEROS_KEYTAB    = "ranger.client.kerberos.keytab";
    private static final String CLIENT_SSL_CONFIG_FILE    = "ranger.client.ssl.config.filename";


    private final Properties props;

    RangerClientConfig(String configFileName){
        props = readProperties(configFileName);
    }

    public Properties readProperties(String fileName) {
        Properties  ret     = null;
        InputStream inStr   = null;
        URL         fileURL = null;
        File        f       = new File(fileName);

        if (f.exists() && f.isFile() && f.canRead()) {
            try {
                inStr   = new FileInputStream(f);
                fileURL = f.toURI().toURL();
            } catch (FileNotFoundException exception) {
                LOG.error("Error processing input file:" + fileName + " or no privilege for reading file " + fileName, exception);
            } catch (MalformedURLException malformedException) {
                LOG.error("Error processing input file:" + fileName + " cannot be converted to URL " + fileName, malformedException);
            }
        } else {
            fileURL = getClass().getResource(fileName);

            if (fileURL == null && !fileName.startsWith("/")) {
                fileURL = getClass().getResource("/" + fileName);
            }

            if (fileURL == null) {
                fileURL = ClassLoader.getSystemClassLoader().getResource(fileName);

                if (fileURL == null && !fileName.startsWith("/")) {
                    fileURL = ClassLoader.getSystemClassLoader().getResource("/" + fileName);
                }
            }
        }

        if (fileURL != null) {
            try {
                inStr = fileURL.openStream();

                Properties prop = new Properties();

                prop.load(inStr);

                ret = prop;
            } catch (Exception excp) {
                LOG.error("failed to load properties from file '" + fileName + "'", excp);
            } finally {
                if (inStr != null) {
                    try {
                        inStr.close();
                    } catch (Exception excp) {
                        // ignore
                    }
                }
            }
        }
        return ret;
    }
    public String getURL() { return props.getProperty(RANGER_ADMIN_URL); }

    public String getPrincipal(){
        return props.getProperty(CLIENT_KERBEROS_PRINCIPAL);
    }

    public String getKeytab(){
        return props.getProperty(CLIENT_KERBEROS_KEYTAB);
    }

    public String getSslConfigFile(){
        return props.getProperty(CLIENT_SSL_CONFIG_FILE);
    }

    public String getAuthenticationType(){
        return props.getProperty(AUTH_TYPE);
    }

}
