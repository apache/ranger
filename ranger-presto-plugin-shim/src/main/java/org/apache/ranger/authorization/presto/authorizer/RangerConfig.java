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
package org.apache.ranger.authorization.presto.authorizer;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class RangerConfig {
  private String keytab;
  private String principal;

  public String getKeytab() { return keytab; }

  @Config("ranger.keytab")
  @ConfigDescription("Keytab for authentication against Ranger")
  @SuppressWarnings("unused")
  public RangerConfig setKeytab(String keytab) {
    this.keytab = keytab;
    return this;
  }

  public String getPrincipal() { return principal; }

  @Config("ranger.principal")
  @ConfigDescription("Principal for authentication against Ranger with keytab")
  @SuppressWarnings("unused")
  public RangerConfig setPrincipal(String principal) {
    this.principal = principal;
    return this;
  }
}
