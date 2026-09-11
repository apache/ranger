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

package org.apache.ranger.plugin.authn;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDefaultJwtProvider {
    @Test
    public void defaultJwtProviderIsSupplierAndReadsTokenFromFile() throws Exception {
        assertTrue(Supplier.class.isAssignableFrom(DefaultJwtProvider.class), "DefaultJwtProvider should implement Supplier<String>");

        File jwtFile = File.createTempFile("ranger-jwt", ".token");

        jwtFile.deleteOnExit();

        Files.write(jwtFile.toPath(), "file-jwt-token".getBytes());

        Configuration config = new Configuration();

        config.set("test.prefix" + DefaultJwtProvider.JWT_SOURCE, "file");
        config.set("test.prefix" + DefaultJwtProvider.JWT_FILE, jwtFile.getAbsolutePath());

        Supplier<String> provider = new DefaultJwtProvider("test.prefix", config);

        assertEquals("file-jwt-token", provider.get());
    }
}
