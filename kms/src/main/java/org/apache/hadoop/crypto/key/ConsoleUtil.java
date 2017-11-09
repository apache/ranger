/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.crypto.key;

import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

/**
 * Utility class for reading passwords from the console.
 *
 */
class ConsoleUtil {

    /**
     * Ask a password from console, and return as a char array.
     * @param prompt the question which is prompted
     * @return the password.
     */
    static char[] getPasswordFromConsole(String prompt) throws IOException {
        return getStringPasswordFromConsole(prompt).toCharArray();
    }

    /**
     * Ask a password from console, and return as a String.
     * @param prompt the question which is prompted
     * @return the password.
     */
    static String getStringPasswordFromConsole(String prompt) throws IOException {
        String ret = null;
        Console c = System.console();
        if (c == null) {
            System.out.print(prompt + " ");
            InputStream in = System.in;
            int max = 50;
            byte[] b = new byte[max];
            int l = in.read(b);
            l--; // last character is \n
            if (l > 0) {
                byte[] e = new byte[l];
                System.arraycopy(b, 0, e, 0, l);
                ret = new String(e, Charset.defaultCharset());
            }
        } else {
            char[] pwd = c.readPassword(prompt + " ");
            if (pwd == null) {
                ret = null;
            } else {
                ret = new String(pwd);
            }
        }
        if (ret == null) {
            ret = "";
        }
        return ret;
    }

}
