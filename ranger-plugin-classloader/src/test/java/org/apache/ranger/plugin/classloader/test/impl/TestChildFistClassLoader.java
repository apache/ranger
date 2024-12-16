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

package org.apache.ranger.plugin.classloader.test.impl;

import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.apache.ranger.plugin.classloader.test.TestPlugin;
import org.apache.ranger.plugin.classloader.test.TestPrintParent;

import java.io.File;

public class TestChildFistClassLoader {
    private TestChildFistClassLoader() {
        // to block instantiation
    }

    public static void main(String[] args) {
        TestPrintParent testPrint = new TestPrintParent();

        System.out.println(testPrint.getString());

        try {
            File file = new File(".." + File.separatorChar + "TestPluginImpl.class");

            file.toPath().toUri().toURL();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            @SuppressWarnings("resource")
            RangerPluginClassLoader rangerPluginClassLoader = new RangerPluginClassLoader("hdfs", TestChildFistClassLoader.class);

            TestPlugin testPlugin = (TestPlugin) rangerPluginClassLoader.loadClass("org.apache.ranger.plugin.classloader.test.impl.TestPluginImpl").newInstance();

            System.out.println(testPlugin.print());
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
