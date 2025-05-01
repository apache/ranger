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
package org.apache.ranger.db.upgrade;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.net.URL;

@Component("test")
@Lazy
public class DummyConfigProviderForTest implements IConfigProvider {
    public static String getResourceFilePath(String resourceRelativePath) {
        // Get the resource URL
        ClassLoader classLoader = DummyConfigProviderForTest.class.getClassLoader();
        URL         resourceURL = classLoader.getResource(resourceRelativePath);
        if (resourceURL == null) {
            throw new IllegalArgumentException("Resource not found: " + resourceRelativePath);
        }
        return resourceURL.getFile();
    }

    @Override
    public String getUrl() {
        return "url";
    }

    @Override
    public String getUsername() {
        return "user";
    }

    @Override
    public String getPassword() {
        return "pwd";
    }

    @Override
    public String getDriver() {
        return "driver";
    }

    @Override
    public String getMasterChangelogRelativePath() {
        return getResourceFilePath("masterChangelogForTest.txt");
    }

    @Override
    public String getFinalizeChangelogRelativePath() {
        return getResourceFilePath("finalizeChangelogForTest.txt");
    }
}
