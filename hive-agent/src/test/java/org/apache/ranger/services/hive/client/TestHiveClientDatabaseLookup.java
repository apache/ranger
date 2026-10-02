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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.services.hive.client;

import org.apache.ranger.plugin.client.BaseClient;
import org.junit.jupiter.api.Test;

import javax.security.auth.Subject;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHiveClientDatabaseLookup {
    @Test
    public void testConnectionPatternUsesJdbcWildcard() throws Exception {
        Connection       connection = mock(Connection.class);
        DatabaseMetaData metadata   = mock(DatabaseMetaData.class);
        ResultSet        resultSet  = mock(ResultSet.class);
        HiveClient       client     = createClient(connection);

        when(connection.getMetaData()).thenReturn(metadata);
        when(metadata.getSchemas(null, "%")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("TABLE_SCHEM")).thenReturn("default", "sales_q3");

        assertEquals(Arrays.asList("default", "sales_q3"), client.getDatabaseList("*", null));
        verify(connection, never()).createStatement();
        verify(resultSet).close();
    }

    @Test
    public void testResourceLookupPatternAndExclusions() throws Exception {
        Connection       connection = mock(Connection.class);
        DatabaseMetaData metadata   = mock(DatabaseMetaData.class);
        ResultSet        resultSet  = mock(ResultSet.class);
        HiveClient       client     = createClient(connection);

        when(connection.getMetaData()).thenReturn(metadata);
        when(metadata.getSchemas(null, "sales_%")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, true, false);
        when(resultSet.getString("TABLE_SCHEM")).thenReturn("sales_q3", "salesXq3", "sales_old");

        assertEquals(Collections.singletonList("sales_q3"),
                client.getDatabaseList("sales_*", Collections.singletonList("sales_old")));
        verify(connection, never()).createStatement();
        verify(resultSet).close();
    }

    @Test
    public void testTableLookupPatternAndExclusions() throws Exception {
        Connection       connection = mock(Connection.class);
        DatabaseMetaData metadata   = mock(DatabaseMetaData.class);
        ResultSet        resultSet  = mock(ResultSet.class);
        HiveClient       client     = createClient(connection);

        when(connection.getMetaData()).thenReturn(metadata);
        when(metadata.getTables(null, "impala_test", "test%", null)).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, true, false);
        when(resultSet.getString("TABLE_NAME")).thenReturn("test1", "tasty", "test_old");

        assertEquals(Collections.singletonList("test1"), client.getTableList("test*",
                Collections.singletonList("impala_test"), Collections.singletonList("test_old")));
        verify(connection, never()).createStatement();
        verify(resultSet).close();
    }

    private HiveClient createClient(Connection connection) throws Exception {
        HiveClient client = mock(HiveClient.class, CALLS_REAL_METHODS);
        Field loginSubject = BaseClient.class.getDeclaredField("loginSubject");
        loginSubject.setAccessible(true);
        loginSubject.set(client, new Subject());
        Field jdbcConnection = HiveClient.class.getDeclaredField("con");
        jdbcConnection.setAccessible(true);
        jdbcConnection.set(client, connection);
        return client;
    }
}
