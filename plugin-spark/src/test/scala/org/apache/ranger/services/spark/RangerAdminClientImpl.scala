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

package org.apache.ranger.services.spark

import java.nio.file.{Files, FileSystems}
import java.util

import com.google.gson.GsonBuilder
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.ranger.admin.client.RangerAdminClient
import org.apache.ranger.plugin.util.{GrantRevokeRequest, ServicePolicies, ServiceTags}

class RangerAdminClientImpl extends RangerAdminClient {
  private val LOG: Log = LogFactory.getLog(classOf[RangerAdminClientImpl])
  private val cacheFilename = "sparkSql_hive_jenkins.json"
  private val gson =
    new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create

  override def init(serviceName: String, appId: String, configPropertyPrefix: String): Unit = {}

  override def getServicePoliciesIfUpdated(
      lastKnownVersion: Long,
      lastActivationTimeInMillis: Long): ServicePolicies = {
    val basedir = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    val cachePath = FileSystems.getDefault.getPath(basedir, cacheFilename)
    LOG.info("Reading policies from " + cachePath)
    val bytes = Files.readAllBytes(cachePath)
    gson.fromJson(new String(bytes), classOf[ServicePolicies])
  }

  override def grantAccess(request: GrantRevokeRequest): Unit = {}

  override def revokeAccess(request: GrantRevokeRequest): Unit = {}

  override def getServiceTagsIfUpdated(
      lastKnownVersion: Long,
      lastActivationTimeInMillis: Long): ServiceTags = null

  override def getTagTypes(tagTypePattern: String): util.List[String] = null
}
