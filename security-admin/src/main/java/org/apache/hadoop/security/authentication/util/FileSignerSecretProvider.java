package org.apache.hadoop.security.authentication.util;

import jakarta.servlet.ServletContext;

import java.util.Properties;

public class FileSignerSecretProvider extends SignerSecretProvider {

    public void init(Properties var1, ServletContext var2, long var3) throws Exception {

    }

    public void destroy() {
    }

    public byte[] getCurrentSecret() {
        return null;
    }

    public byte[][] getAllSecrets() {
        return null;
    }
}
