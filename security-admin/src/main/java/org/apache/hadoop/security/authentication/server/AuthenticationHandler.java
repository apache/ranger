package org.apache.hadoop.security.authentication.server;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import java.io.IOException;
import java.util.Properties;

public interface AuthenticationHandler {
    String WWW_AUTHENTICATE = "WWW-Authenticate";

    String getType();

    void init(Properties var1) throws ServletException;

    void destroy();

    boolean managementOperation(AuthenticationToken var1, HttpServletRequest var2, HttpServletResponse var3) throws IOException, AuthenticationException;

    AuthenticationToken authenticate(HttpServletRequest var1, HttpServletResponse var2) throws IOException, AuthenticationException;
}


