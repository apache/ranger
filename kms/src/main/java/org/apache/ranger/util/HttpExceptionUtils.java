package org.apache.ranger.util;

import jakarta.ws.rs.core.Response;

import java.util.LinkedHashMap;
import java.util.Map;

public class HttpExceptionUtils {

    private static final String ENTER = System.getProperty("line.separator");

    public static Response createJerseyExceptionResponse(Response.Status status, Throwable ex) {
        Map<String, Object> json = new LinkedHashMap();
        json.put("message", getOneLineMessage(ex));
        json.put("exception", ex.getClass().getSimpleName());
        json.put("javaClassName", ex.getClass().getName());
        Map<String, Object> response = new LinkedHashMap();
        response.put("RemoteException", json);
        return Response.status(status).type("application/json").entity(response).build();
    }

    private static String getOneLineMessage(Throwable exception) {
        String message = exception.getMessage();
        if (message != null) {
            int i = message.indexOf(ENTER);
            if (i > -1) {
                message = message.substring(0, i);
            }
        }

        return message;
    }

}
