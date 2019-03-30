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

package org.apache.ranger.authorization.spark.authorizer

import java.util.Date

import java.util.{Set => JSet}

import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveAuthzContext, HiveAuthzSessionContext, HiveOperationType}
import org.apache.ranger.authorization.spark.authorizer.SparkAccessType.SparkAccessType
import org.apache.ranger.plugin.policyengine.{RangerAccessRequestImpl, RangerPolicyEngine}
import org.apache.ranger.plugin.util.RangerAccessRequestUtil

class RangerSparkAccessRequest private extends RangerAccessRequestImpl {

  private var accessType = SparkAccessType.NONE

  def this(
      resource: RangerSparkResource,
      user: String,
      userGroups: JSet[String],
      hiveOpTypeName: String,
      accessType: SparkAccessType,
      context: HiveAuthzContext,
      sessionContext: HiveAuthzSessionContext,
      clusterName: String) {
    this()
    this.setResource(resource)
    this.setUser(user)
    this.setUserGroups(userGroups)
    this.setAccessTime(new Date)
    this.setAction(hiveOpTypeName)
    this.setSparkAccessType(accessType)
    this.setUser(user)
    if (context != null) {
      this.setRequestData(context.getCommandString)
      this.setRemoteIPAddress(context.getIpAddress)
    }
    if (sessionContext != null) {
      this.setClientType(Option(sessionContext.getClientType).map(_.toString).orNull)
      this.setSessionId(sessionContext.getSessionString)
    }
    this.setClusterName(clusterName)
  }

  def this(
      resource: RangerSparkResource,
      user: String,
      userGroups: JSet[String],
      hiveOpType: HiveOperationType,
      accessType: SparkAccessType,
      context: HiveAuthzContext,
      sessionContext: HiveAuthzSessionContext,
      clusterName: String) = {
    this(resource, user, userGroups, hiveOpType.name(), accessType, context, sessionContext,
      clusterName)
  }

  def this(
      resource: RangerSparkResource,
      user: String,
      userGroups: JSet[String],
      context: HiveAuthzContext,
      sessionContext: HiveAuthzSessionContext,
      clusterName: String) = {
    this(resource, user, userGroups, "METADATA OPERATION", SparkAccessType.USE, context,
      sessionContext, clusterName)
  }

  def getSparkAccessType: SparkAccessType = accessType

  def setSparkAccessType(accessType: SparkAccessType): Unit = {
    this.accessType = accessType
    accessType match {
      case SparkAccessType.USE => this.setAccessType(RangerPolicyEngine.ANY_ACCESS)
      case SparkAccessType.ADMIN => this.setAccessType(RangerPolicyEngine.ADMIN_ACCESS)
      case _ => this.setAccessType(accessType.toString.toLowerCase)
    }
  }

  def copy(): RangerSparkAccessRequest = {
    val ret = new RangerSparkAccessRequest()
    ret.setResource(getResource)
    ret.setAccessType(getAccessType)
    ret.setUser(getUser)
    ret.setUserGroups(getUserGroups)
    ret.setAccessTime(getAccessTime)
    ret.setAction(getAction)
    ret.setClientIPAddress(getClientIPAddress)
    ret.setRemoteIPAddress(getRemoteIPAddress)
    ret.setForwardedAddresses(getForwardedAddresses)
    ret.setRequestData(getRequestData)
    ret.setClientType(getClientType)
    ret.setSessionId(getSessionId)
    ret.setContext(RangerAccessRequestUtil.copyContext(getContext))
    ret.accessType = accessType
    ret.setClusterName(getClusterName)
    ret
  }
}
