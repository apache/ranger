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
package org.apache.ranger.security.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;

import java.io.InputStream;

/**
 * Activates the "saml" Spring profile when ranger.authentication.method=SAML.
 * This ensures OpenSAML beans are only instantiated when SAML is configured,
 * allowing non-SAML deployments to run safely on JDK 8.
 * <p>
 * IMPORTANT: We cannot use PropertiesUtil here because it is populated by a
 * BeanFactoryPostProcessor that runs after ApplicationContextInitializer.
 */
public class RangerApplicationContextInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    private static final Logger LOG = LoggerFactory.getLogger(RangerApplicationContextInitializer.class);

    private static final String SAML_AUTH_METHOD = "SAML";
    private static final String SAML_PROFILE = "saml";
    private static final String AUTH_METHOD_PROPERTY = "ranger.authentication.method";
    private static final String CONFIG_RESOURCE = "ranger-admin-site.xml";

    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        String authMethod = readAuthMethodFromConfigFile();

        if (SAML_AUTH_METHOD.equalsIgnoreCase(authMethod)) {
            // Critical safety check for JDK compatibility
            try {
                Class.forName("org.opensaml.saml.saml2.core.AuthnRequest", false, getClass().getClassLoader());
            } catch (ClassNotFoundException | UnsupportedClassVersionError e) {
                throw new IllegalStateException(
                        "SAML 2.0 authentication is enabled (ranger.authentication.method=SAML), " +
                                "but it requires Java 11 or higher. OpenSAML 4.x is not compatible with Java 8. " +
                                "Please upgrade Ranger Admin JVM to Java 11+ or change the authentication method.", e);
            }
            LOG.info("RangerApplicationContextInitializer: activating '{}' Spring profile (ranger.authentication.method={})", SAML_PROFILE, authMethod);
            applicationContext.getEnvironment().addActiveProfile(SAML_PROFILE);
        } else {
            LOG.info("RangerApplicationContextInitializer: SAML profile not activated (ranger.authentication.method={})", authMethod);
        }
    }

    /**
     * Reads ranger.authentication.method directly from ranger-admin-site.xml on the
     * classpath.  This avoids the PropertiesUtil dependency, which is not yet
     * initialised at ApplicationContextInitializer time.
     *
     * @return the configured authentication method, or {@code "NONE"} if the
     * property is absent or the file cannot be read.
     */
    private String readAuthMethodFromConfigFile() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(CONFIG_RESOURCE)) {
            if (is == null) {
                LOG.warn("RangerApplicationContextInitializer: {} not found on classpath; defaulting to NONE", CONFIG_RESOURCE);
                return "NONE";
            }

            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            // Harden against XXE
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
            dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            dbf.setExpandEntityReferences(false);
            dbf.setNamespaceAware(false);

            Document doc = dbf.newDocumentBuilder().parse(is);
            NodeList properties = doc.getElementsByTagName("property");

            for (int i = 0; i < properties.getLength(); i++) {
                Element property = (Element) properties.item(i);
                NodeList names = property.getElementsByTagName("name");

                if (names.getLength() > 0 &&
                        AUTH_METHOD_PROPERTY.equals(names.item(0).getTextContent().trim())) {
                    NodeList values = property.getElementsByTagName("value");

                    if (values.getLength() > 0) {
                        String value = values.item(0).getTextContent().trim();
                        LOG.info("RangerApplicationContextInitializer: read {}={} from {}", AUTH_METHOD_PROPERTY, value, CONFIG_RESOURCE);
                        return value;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("RangerApplicationContextInitializer: failed to read {} — defaulting to NONE", CONFIG_RESOURCE, e);
        }
        LOG.warn("RangerApplicationContextInitializer: {} not found in {}; defaulting to NONE", AUTH_METHOD_PROPERTY, CONFIG_RESOURCE);
        return "NONE";
    }
}
