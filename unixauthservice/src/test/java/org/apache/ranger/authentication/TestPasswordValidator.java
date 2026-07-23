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

package org.apache.ranger.authentication;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit Test cases for PasswordValidator
 */

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestPasswordValidator {
    private static LoginAttemptTracker permissiveTracker() {
        return new LoginAttemptTracker(1000, 60_000, 30_000);
    }

    @Test
    public void test00_staticGettersAndSetters() {
        PasswordValidator.setValidatorProgram("/tmp/prog");
        PasswordValidator.setAdminUserList(Collections.singletonList("root"));
        PasswordValidator.setAdminRoleNames("ROLE_SUPER");

        assertEquals("/tmp/prog", PasswordValidator.getValidatorProgram());
        assertEquals(Collections.singletonList("root"), PasswordValidator.getAdminUserList());
        assertEquals("ROLE_SUPER", PasswordValidator.getAdminRoleNames());
    }

    @Test
    public void test01_nullProgramWritesFailedAndClosesSocket() throws Exception {
        PasswordValidator.setValidatorProgram(null);
        PasswordValidator.setAdminUserList(null);
        PasswordValidator.setAdminRoleNames(null);

        ByteArrayOutputStream out    = new ByteArrayOutputStream();
        Socket                socket = buildSocket("LOGIN: alice secret", out);

        new PasswordValidator(socket, permissiveTracker()).run();

        String response = out.toString(StandardCharsets.UTF_8.name()).trim();
        assertEquals("FAILED: Authentication failed.", response);
        verify(socket, times(1)).close();
    }

    @Test
    public void test02_okResponseAppendsAdminRoleAndDestroysProcess() throws Exception {
        PasswordValidator.setValidatorProgram("/bin/validator");
        PasswordValidator.setAdminUserList(Arrays.asList("alice", "bob"));
        PasswordValidator.setAdminRoleNames("ROLE_ADMIN");

        ByteArrayOutputStream out    = new ByteArrayOutputStream();
        Socket                socket = buildSocket("LOGIN: alice secret", out);

        Process              process       = mock(Process.class);
        ByteArrayInputStream processStdout = new ByteArrayInputStream("OK".getBytes(StandardCharsets.UTF_8));
        when(process.getInputStream()).thenReturn(processStdout);
        when(process.getOutputStream()).thenReturn(new ByteArrayOutputStream());

        try (MockedStatic<Runtime> runtimeStatic = mockStatic(Runtime.class)) {
            Runtime rt = mock(Runtime.class);
            runtimeStatic.when(Runtime::getRuntime).thenReturn(rt);
            when(rt.exec("/bin/validator")).thenReturn(process);

            new PasswordValidator(socket, permissiveTracker()).run();
        }

        String response = out.toString(StandardCharsets.UTF_8).trim();
        assertEquals("OK ROLE_ADMIN", response);
        verify(process, times(1)).destroy();
        verify(socket, times(1)).close();
    }

    @Test
    public void test03_okResponseWithoutAdminDoesNotAppendRole() throws Exception {
        PasswordValidator.setValidatorProgram("/bin/validator");
        PasswordValidator.setAdminUserList(Collections.singletonList("bob"));
        PasswordValidator.setAdminRoleNames("ROLE_ADMIN");

        ByteArrayOutputStream out    = new ByteArrayOutputStream();
        Socket                socket = buildSocket("LOGIN: alice secret", out);

        Process              process       = mock(Process.class);
        ByteArrayInputStream processStdout = new ByteArrayInputStream("OK".getBytes(StandardCharsets.UTF_8));
        when(process.getInputStream()).thenReturn(processStdout);
        when(process.getOutputStream()).thenReturn(new ByteArrayOutputStream());

        try (MockedStatic<Runtime> runtimeStatic = mockStatic(Runtime.class)) {
            Runtime rt = mock(Runtime.class);
            runtimeStatic.when(Runtime::getRuntime).thenReturn(rt);
            when(rt.exec("/bin/validator")).thenReturn(process);

            new PasswordValidator(socket, permissiveTracker()).run();
        }

        String response = out.toString(StandardCharsets.UTF_8.name()).trim();
        assertEquals("OK", response);
        verify(process, times(1)).destroy();
        verify(socket, times(1)).close();
    }

    @Test
    public void test04_execThrowsWritesFailed() throws Exception {
        PasswordValidator.setValidatorProgram("/bin/validator");
        PasswordValidator.setAdminUserList(Collections.singletonList("alice"));
        PasswordValidator.setAdminRoleNames("ROLE_ADMIN");

        ByteArrayOutputStream out    = new ByteArrayOutputStream();
        Socket                socket = buildSocket("LOGIN: alice secret", out);

        try (MockedStatic<Runtime> runtimeStatic = mockStatic(Runtime.class)) {
            Runtime rt = mock(Runtime.class);
            runtimeStatic.when(Runtime::getRuntime).thenReturn(rt);
            when(rt.exec("/bin/validator")).thenThrow(new IOException("boom"));

            new PasswordValidator(socket, permissiveTracker()).run();
        }

        String response = out.toString(StandardCharsets.UTF_8).trim();
        assertEquals("FAILED: Authentication failed.", response);
        verify(socket, times(1)).close();
    }

    @Test
    public void test05_closeSocketIOExceptionHandled() throws Exception {
        PasswordValidator.setValidatorProgram(null);
        PasswordValidator.setAdminUserList(null);
        PasswordValidator.setAdminRoleNames(null);

        ByteArrayOutputStream out    = new ByteArrayOutputStream();
        Socket                socket = buildSocket("LOGIN: alice secret", out);
        doThrow(new IOException("close failure")).when(socket).close();

        new PasswordValidator(socket, permissiveTracker()).run();

        // No exception should be thrown even if close() fails; nothing to assert besides method completed
        verify(socket, times(1)).close();
    }

    @Test
    public void test06_blockedIpSkipsValidatorAndReturnsLockedOutResponse() throws Exception {
        PasswordValidator.setValidatorProgram("/bin/validator");
        PasswordValidator.setAdminUserList(null);
        PasswordValidator.setAdminRoleNames(null);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Socket socket = buildSocket("LOGIN: alice secret", out);
        LoginAttemptTracker tracker = mock(LoginAttemptTracker.class);
        when(tracker.isBlocked(anyString())).thenReturn(true);
        new PasswordValidator(socket, tracker).run();
        String response = out.toString(StandardCharsets.UTF_8).trim();
        assertEquals("FAILED: Too many attempts. Try again later.", response);
        verify(socket, times(1)).close();
    }

    @Test
    public void test07_failedValidatorResponseRecordsFailureOnTracker() throws Exception {
        PasswordValidator.setValidatorProgram("/bin/validator");
        PasswordValidator.setAdminUserList(null);
        PasswordValidator.setAdminRoleNames(null);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Socket socket = buildSocket("LOGIN: alice secret", out);

        Process process = mock(Process.class);
        ByteArrayInputStream processStdout = new ByteArrayInputStream("FAILED: Password did not match.".getBytes(StandardCharsets.UTF_8));
        when(process.getInputStream()).thenReturn(processStdout);
        when(process.getOutputStream()).thenReturn(new ByteArrayOutputStream());

        LoginAttemptTracker tracker = mock(LoginAttemptTracker.class);
        when(tracker.isBlocked(anyString())).thenReturn(false);
        when(tracker.getAccountDelayMs(any(), anyString())).thenReturn(0L);

        try (MockedStatic<Runtime> runtimeStatic = mockStatic(Runtime.class)) {
            Runtime rt = mock(Runtime.class);
            runtimeStatic.when(Runtime::getRuntime).thenReturn(rt);
            when(rt.exec("/bin/validator")).thenReturn(process);
            new PasswordValidator(socket, tracker).run();
        }

        String response = out.toString(StandardCharsets.UTF_8).trim();
        assertEquals("FAILED: Authentication failed.", response);
        verify(tracker, times(1)).recordFailure(anyString(), anyString());
        verify(tracker, never()).recordSuccess(anyString(), anyString());
    }

    @Test
    public void test08_okValidatorResponseRecordsSuccessOnTracker() throws Exception {
        PasswordValidator.setValidatorProgram("/bin/validator");
        PasswordValidator.setAdminUserList(null);
        PasswordValidator.setAdminRoleNames(null);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Socket socket = buildSocket("LOGIN: alice secret", out);

        Process process = mock(Process.class);
        ByteArrayInputStream processStdout = new ByteArrayInputStream("OK".getBytes(StandardCharsets.UTF_8));
        when(process.getInputStream()).thenReturn(processStdout);
        when(process.getOutputStream()).thenReturn(new ByteArrayOutputStream());

        LoginAttemptTracker tracker = mock(LoginAttemptTracker.class);
        when(tracker.isBlocked(anyString())).thenReturn(false);
        when(tracker.getAccountDelayMs(any(), anyString())).thenReturn(0L);

        try (MockedStatic<Runtime> runtimeStatic = mockStatic(Runtime.class)) {
            Runtime rt = mock(Runtime.class);
            runtimeStatic.when(Runtime::getRuntime).thenReturn(rt);
            when(rt.exec("/bin/validator")).thenReturn(process);

            new PasswordValidator(socket, tracker).run();
        }

        String response = out.toString(StandardCharsets.UTF_8).trim();
        assertEquals("OK", response);
        verify(tracker, times(1)).recordSuccess(anyString(), anyString());
        verify(tracker, never()).recordFailure(anyString(), anyString());
    }

    @Test
    public void test09_accountDelayStillInvokesValidator() throws Exception {
        PasswordValidator.setValidatorProgram("/bin/validator");
        PasswordValidator.setAdminUserList(null);
        PasswordValidator.setAdminRoleNames(null);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Socket socket = buildSocket("LOGIN: alice secret", out);
        LoginAttemptTracker tracker = mock(LoginAttemptTracker.class);
        when(tracker.isBlocked(anyString())).thenReturn(false);
        when(tracker.getAccountDelayMs(eq("alice"), anyString())).thenReturn(1L);

        Process process = mock(Process.class);
        ByteArrayInputStream processStdout = new ByteArrayInputStream("OK".getBytes(StandardCharsets.UTF_8));
        when(process.getInputStream()).thenReturn(processStdout);
        when(process.getOutputStream()).thenReturn(new ByteArrayOutputStream());

        try (MockedStatic<Runtime> runtimeStatic = mockStatic(Runtime.class)) {
            Runtime rt = mock(Runtime.class);
            runtimeStatic.when(Runtime::getRuntime).thenReturn(rt);
            when(rt.exec("/bin/validator")).thenReturn(process);
            new PasswordValidator(socket, tracker).run();
        }

        String response = out.toString(StandardCharsets.UTF_8.name()).trim();
        assertEquals("OK", response);
        verify(tracker, never()).isBlocked(anyString(), any());
    }

    @Test
    public void test10_adminUserSkipsAccountTracking() throws Exception {
        PasswordValidator.setValidatorProgram("/bin/validator");
        PasswordValidator.setAdminUserList(Collections.singletonList("admin"));
        PasswordValidator.setAdminRoleNames("ROLE_ADMIN");

        LoginAttemptTracker tracker = new LoginAttemptTracker(100, 60_000, 30_000, true, 3, 60_000, 500, 5_000);
        for (int i = 0; i < 5; i++) {
            tracker.recordFailure("10.0.0." + i, "alice");
        }
        assertTrue(tracker.getAccountDelayMs("alice", "10.0.0.99") > 0);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Socket socket = buildSocket("LOGIN: admin secret", out);

        Process process = mock(Process.class);
        ByteArrayInputStream processStdout = new ByteArrayInputStream("FAILED: bad".getBytes(StandardCharsets.UTF_8));
        when(process.getInputStream()).thenReturn(processStdout);
        when(process.getOutputStream()).thenReturn(new ByteArrayOutputStream());

        LoginAttemptTracker adminTracker = new LoginAttemptTracker(100, 60_000, 30_000, true, 3, 60_000, 500, 5_000);

        try (MockedStatic<Runtime> runtimeStatic = mockStatic(Runtime.class)) {
            Runtime rt = mock(Runtime.class);
            runtimeStatic.when(Runtime::getRuntime).thenReturn(rt);
            when(rt.exec("/bin/validator")).thenReturn(process);

            new PasswordValidator(socket, adminTracker).run();
        }

        assertEquals(0, adminTracker.getAccountDelayMs("admin", "10.0.0.1"));
        verify(process, times(1)).destroy();
    }

    private Socket buildSocket(String requestLine, ByteArrayOutputStream responseOut) throws IOException {
        Socket       socket = mock(Socket.class);
        InputStream  in     = new ByteArrayInputStream((requestLine + "\n").getBytes(StandardCharsets.UTF_8));
        OutputStream out    = responseOut;
        when(socket.getInputStream()).thenReturn(in);
        when(socket.getOutputStream()).thenReturn(out);
        return socket;
    }
}
