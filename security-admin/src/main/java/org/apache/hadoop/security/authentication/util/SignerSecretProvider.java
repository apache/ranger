package org.apache.hadoop.security.authentication.util;

import jakarta.servlet.ServletContext;
import java.util.Properties;

public abstract class SignerSecretProvider {
    public SignerSecretProvider() {
    }

    public abstract void init(Properties var1, ServletContext var2, long var3) throws Exception;

    public void destroy() {
    }

    public abstract byte[] getCurrentSecret();

    public abstract byte[][] getAllSecrets();
}
