/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.authorization.trino.authorizer;

import org.apache.ranger.admin.client.AbstractRangerAdminClient;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;

public class RangerAdminClientImpl extends AbstractRangerAdminClient {
  private final static String cacheFilename = "trino-policies.json";

  public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {

    String basedir = System.getProperty("basedir");
    if (basedir == null) {
      basedir = new File(".").getCanonicalPath();
    }

    java.nio.file.Path cachePath = FileSystems.getDefault().getPath(basedir, "/src/test/resources/" + cacheFilename);
    byte[] cacheBytes = Files.readAllBytes(cachePath);

    return gson.fromJson(new String(cacheBytes), ServicePolicies.class);
  }

}