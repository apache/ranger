/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.services.trino.client;

import org.apache.ranger.plugin.client.HadoopException;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.security.auth.Subject;

import java.lang.reflect.Field;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLTimeoutException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestTrinoClient {
    private TrinoClient createMockedClient() throws Exception {
        return createMockedClient(new HashMap<>());
    }

    private TrinoClient createMockedClient(Map<String, String> props) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("username", "test");
        config.put("password", "test");
        config.put("jdbc.driverClassName", "io.trino.jdbc.TrinoDriver");
        config.put("jdbc.url", "jdbc:trino://localhost:8080");
        config.putAll(props);

        NoConnectionTrinoClient client = new NoConnectionTrinoClient("svc", config);
        return client;
    }

    @Test
    public void test01_getCatalogList_normalOperation() throws Exception {
        try (MockedStatic<DriverManager> dmStatic = Mockito.mockStatic(DriverManager.class);
                MockedStatic<Subject> subjectStatic = Mockito.mockStatic(Subject.class)) {
            Connection mockCon = mock(Connection.class);
            DatabaseMetaData metadata = mock(DatabaseMetaData.class);
            ResultSet rs = mock(ResultSet.class);
            dmStatic.when(() -> DriverManager.getConnection(anyString(), any())).thenReturn(mockCon);
            when(mockCon.getMetaData()).thenReturn(metadata);
            when(metadata.getCatalogs()).thenReturn(rs);
            when(rs.next()).thenReturn(true, true, false);
            when(rs.getString("TABLE_CAT")).thenReturn("catalog1", "catalog2");
            subjectStatic.when(() -> Subject.doAs(any(), any(PrivilegedAction.class)))
                    .thenAnswer(inv -> {
                        PrivilegedAction<?> action = inv.getArgument(1);
                        return action.run();
                    });

            TrinoClient client = createMockedClient();
            List<String> out = client.getCatalogList("*", null);
            assertEquals(Arrays.asList("catalog1", "catalog2"), out);
        }
    }

    @Test
    public void test02_getCatalogList_withExcludeList() throws Exception {
        try (MockedStatic<DriverManager> dmStatic = Mockito.mockStatic(DriverManager.class);
                MockedStatic<Subject> subjectStatic = Mockito.mockStatic(Subject.class)) {
            Connection mockCon = mock(Connection.class);
            DatabaseMetaData metadata = mock(DatabaseMetaData.class);
            ResultSet rs = mock(ResultSet.class);
            dmStatic.when(() -> DriverManager.getConnection(anyString(), any())).thenReturn(mockCon);
            when(mockCon.getMetaData()).thenReturn(metadata);
            when(metadata.getCatalogs()).thenReturn(rs);
            when(rs.next()).thenReturn(true, true, false);
            when(rs.getString("TABLE_CAT")).thenReturn("catalog1", "catalog2");
            subjectStatic.when(() -> Subject.doAs(any(), any(PrivilegedAction.class)))
                    .thenAnswer(inv -> {
                        PrivilegedAction<?> action = inv.getArgument(1);
                        return action.run();
                    });

            TrinoClient client = createMockedClient();
            List<String> out = client.getCatalogList("*", Collections.singletonList("catalog1"));
            assertEquals(Collections.singletonList("catalog2"), out);
        }
    }

    @Test
    public void test03_getCatalogList_timeoutThrowsHadoopException() throws Exception {
        try (MockedStatic<DriverManager> dmStatic = Mockito.mockStatic(DriverManager.class);
                MockedStatic<Subject> subjectStatic = Mockito.mockStatic(Subject.class)) {
            Connection mockCon = mock(Connection.class);
            DatabaseMetaData metadata = mock(DatabaseMetaData.class);
            dmStatic.when(() -> DriverManager.getConnection(anyString(), any())).thenReturn(mockCon);
            when(mockCon.getMetaData()).thenReturn(metadata);
            when(metadata.getCatalogs()).thenThrow(SQLTimeoutException.class);
            subjectStatic.when(() -> Subject.doAs(any(), any(PrivilegedAction.class)))
                    .thenAnswer(inv -> {
                        PrivilegedAction<?> action = inv.getArgument(1);
                        return action.run();
                    });

            TrinoClient client = createMockedClient();
            assertThrows(HadoopException.class, () -> client.getCatalogList("*", null));
        }
    }

    @Test
    public void test04_validateSqlIdentifier_rejectsSqlInjection() throws Exception {
        try (MockedStatic<DriverManager> dmStatic = Mockito.mockStatic(DriverManager.class);
                MockedStatic<Subject> subjectStatic = Mockito.mockStatic(Subject.class)) {
            Connection mockCon = mock(Connection.class);
            dmStatic.when(() -> DriverManager.getConnection(anyString(), any())).thenReturn(mockCon);
            subjectStatic.when(() -> Subject.doAs(any(), any(PrivilegedAction.class)))
                    .thenAnswer(inv -> {
                        PrivilegedAction<?> action = inv.getArgument(1);
                        return action.run();
                    });

            TrinoClient client = createMockedClient();

            List<String> maliciousInputs = Arrays.asList("'; DROP TABLE users; --", "test\" OR 1=1 --", "catalog'; DELETE FROM users; --", "../../../etc/passwd", "test<script>alert(1)</script>");

            for (String input : maliciousInputs) {
                HadoopException ex = assertThrows(HadoopException.class,
                        () -> client.getCatalogList(input, null),
                        "SQL injection attempt should be rejected: " + input);
                assertTrue(ex.getMessage().contains("Invalid"),
                        "Error should indicate invalid input for: " + input);
            }
        }
    }

    @Test
    public void test05_validateSqlIdentifier_rejectsSpecialCharacters() throws Exception {
        try (MockedStatic<DriverManager> dmStatic = Mockito.mockStatic(DriverManager.class);
                MockedStatic<Subject> subjectStatic = Mockito.mockStatic(Subject.class)) {
            Connection mockCon = mock(Connection.class);
            dmStatic.when(() -> DriverManager.getConnection(anyString(), any())).thenReturn(mockCon);
            subjectStatic.when(() -> Subject.doAs(any(), any(PrivilegedAction.class)))
                    .thenAnswer(inv -> {
                        PrivilegedAction<?> action = inv.getArgument(1);
                        return action.run();
                    });

            TrinoClient client = createMockedClient();

            List<String> invalidInputs = Arrays.asList("test@catalog", "catalog#name", "table!name", "name(with)parens");

            for (String input : invalidInputs) {
                HadoopException ex = assertThrows(HadoopException.class,
                        () -> client.getCatalogList(input, null),
                        "Special characters should be rejected: " + input);
                assertTrue(ex.getMessage().contains("Invalid"),
                        "Error should indicate invalid input for: " + input);
            }
        }
    }

    @Test
    public void test06_schemaValidation_rejectsSqlInjection() throws Exception {
        try (MockedStatic<DriverManager> dmStatic = Mockito.mockStatic(DriverManager.class);
                MockedStatic<Subject> subjectStatic = Mockito.mockStatic(Subject.class)) {
            Connection mockCon = mock(Connection.class);
            dmStatic.when(() -> DriverManager.getConnection(anyString(), any())).thenReturn(mockCon);
            subjectStatic.when(() -> Subject.doAs(any(), any(PrivilegedAction.class)))
                    .thenAnswer(inv -> {
                        PrivilegedAction<?> action = inv.getArgument(1);
                        return action.run();
                    });

            TrinoClient client = createMockedClient();
            Field fCon = TrinoClient.class.getDeclaredField("con");
            fCon.setAccessible(true);
            fCon.set(client, mockCon);

            HadoopException ex = assertThrows(HadoopException.class,
                    () -> client.getSchemaList("'; DROP TABLE users; --", Collections.singletonList("catalog1"), null));
            assertTrue(ex.getMessage().contains("Invalid"));
        }
    }

    @Test
    public void test07_tableValidation_rejectsSqlInjection() throws Exception {
        try (MockedStatic<DriverManager> dmStatic = Mockito.mockStatic(DriverManager.class);
                MockedStatic<Subject> subjectStatic = Mockito.mockStatic(Subject.class)) {
            Connection mockCon = mock(Connection.class);
            dmStatic.when(() -> DriverManager.getConnection(anyString(), any())).thenReturn(mockCon);
            subjectStatic.when(() -> Subject.doAs(any(), any(PrivilegedAction.class)))
                    .thenAnswer(inv -> {
                        PrivilegedAction<?> action = inv.getArgument(1);
                        return action.run();
                    });

            TrinoClient client = createMockedClient();

            HadoopException ex = assertThrows(HadoopException.class,
                    () -> client.getTableList("'; DROP TABLE users; --", Collections.singletonList("catalog1"), Collections.singletonList("schema1"), null));
            assertTrue(ex.getMessage().contains("Invalid"));
        }
    }

    @Test
    public void test08_columnValidation_rejectsSqlInjection() throws Exception {
        try (MockedStatic<DriverManager> dmStatic = Mockito.mockStatic(DriverManager.class);
                MockedStatic<Subject> subjectStatic = Mockito.mockStatic(Subject.class)) {
            Connection mockCon = mock(Connection.class);
            dmStatic.when(() -> DriverManager.getConnection(anyString(), any())).thenReturn(mockCon);
            subjectStatic.when(() -> Subject.doAs(any(), any(PrivilegedAction.class)))
                    .thenAnswer(inv -> {
                        PrivilegedAction<?> action = inv.getArgument(1);
                        return action.run();
                    });

            TrinoClient client = createMockedClient();

            HadoopException ex = assertThrows(HadoopException.class,
                    () -> client.getColumnList("'; DROP TABLE users; --", Collections.singletonList("catalog1"), Collections.singletonList("schema1"), Collections.singletonList("table1"), null));
            assertTrue(ex.getMessage().contains("Invalid"));
        }
    }

    @Test
    public void test09_catalogName_validateInList() throws Exception {
        try (MockedStatic<DriverManager> dmStatic = Mockito.mockStatic(DriverManager.class);
                MockedStatic<Subject> subjectStatic = Mockito.mockStatic(Subject.class)) {
            Connection mockCon = mock(Connection.class);
            DatabaseMetaData metadata = mock(DatabaseMetaData.class);
            ResultSet rs = mock(ResultSet.class);
            dmStatic.when(() -> DriverManager.getConnection(anyString(), any())).thenReturn(mockCon);
            when(mockCon.getMetaData()).thenReturn(metadata);
            when(metadata.getSchemas(anyString(), anyString())).thenReturn(rs);
            when(rs.next()).thenReturn(false);
            subjectStatic.when(() -> Subject.doAs(any(), any(PrivilegedAction.class)))
                    .thenAnswer(inv -> {
                        PrivilegedAction<?> action = inv.getArgument(1);
                        return action.run();
                    });

            TrinoClient client = createMockedClient();
            Field fCon = TrinoClient.class.getDeclaredField("con");
            fCon.setAccessible(true);
            fCon.set(client, mockCon);

            HadoopException ex = assertThrows(HadoopException.class,
                    () -> client.getSchemaList("schema1", Arrays.asList("catalog1", "'; DROP TABLE --"), null));
            assertTrue(ex.getMessage().contains("Invalid"));
        }
    }

    @Test
    public void test10_schemaName_validateInList() throws Exception {
        try (MockedStatic<DriverManager> dmStatic = Mockito.mockStatic(DriverManager.class);
                MockedStatic<Subject> subjectStatic = Mockito.mockStatic(Subject.class)) {
            Connection mockCon = mock(Connection.class);
            DatabaseMetaData metadata = mock(DatabaseMetaData.class);
            ResultSet rs = mock(ResultSet.class);
            dmStatic.when(() -> DriverManager.getConnection(anyString(), any())).thenReturn(mockCon);
            when(mockCon.getMetaData()).thenReturn(metadata);
            when(metadata.getTables(anyString(), anyString(), anyString(), any())).thenReturn(rs);
            when(rs.next()).thenReturn(false);
            subjectStatic.when(() -> Subject.doAs(any(), any(PrivilegedAction.class)))
                    .thenAnswer(inv -> {
                        PrivilegedAction<?> action = inv.getArgument(1);
                        return action.run();
                    });

            TrinoClient client = createMockedClient();
            Field fCon = TrinoClient.class.getDeclaredField("con");
            fCon.setAccessible(true);
            fCon.set(client, mockCon);

            HadoopException ex = assertThrows(HadoopException.class,
                    () -> client.getTableList("table1", Collections.singletonList("catalog1"), Arrays.asList("schema1", "'; DROP --"), null));
            assertTrue(ex.getMessage().contains("Invalid"));
        }
    }

    private static class NoConnectionTrinoClient extends TrinoClient {
        public NoConnectionTrinoClient(String serviceName, Map<String, String> connectionProperties) {
            super(serviceName, connectionProperties);
        }

        @Override
        protected Subject getLoginSubject() {
            Subject subject = new Subject();
            return subject;
        }

        @Override
        protected void login() {
        }
    }
}
