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

package org.apache.spark.sql.hive.client

import java.util.{List => JList}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.ql.security.authorization.plugin._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{AuthzUtils, SparkSession}
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.internal.NonClosableMutableURLClassLoader

/**
 * A Tool for Authorizer implementation.
 *
 * The [[SessionState]] generates the authorizer and authenticator, we use these to check
 * the privileges of a Spark LogicalPlan, which is mapped to hive privilege objects and operation
 * type.
 *
 * [[SparkSession]] with hive catalog implemented has its own instance of [[SessionState]]. I am
 * strongly willing to reuse it, but for the reason that it belongs to an isolated classloader
 * which makes it unreachable for us to visit it in Spark's context classloader. So, when
 * [[ClassCastException]] occurs, we turn off [[IsolatedClientLoader]] to use Spark's builtin
 * Hive client jars to generate a new metastore client to replace the original one, once it is
 * generated, will be reused then.
 *
 */
private[sql] object RangerSparkAuthzImpl {

  private val logger = LogFactory.getLog(getClass.getSimpleName.stripSuffix("$"))

  def checkPrivileges(
      spark: SparkSession,
      hiveOpType: HiveOperationType,
      inputObjs: JList[HivePrivilegeObject],
      outputObjs: JList[HivePrivilegeObject],
      context: HiveAuthzContext): Unit = {
    val client = spark.sharedState
      .externalCatalog.asInstanceOf[HiveExternalCatalog]
      .client
    val clientImpl = try {
      client.asInstanceOf[HiveClientImpl]
    } catch {
      case _: ClassCastException =>
        val clientLoader =
          AuthzUtils.getFieldVal(client, "clientLoader").asInstanceOf[IsolatedClientLoader]
        AuthzUtils.setFieldVal(clientLoader, "isolationOn", false)
        AuthzUtils.setFieldVal(clientLoader,
          "classLoader", new NonClosableMutableURLClassLoader(clientLoader.baseClassLoader))
        clientLoader.cachedHive = null
        val newClient = clientLoader.createClient()
        AuthzUtils.setFieldVal(
          spark.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog],
          "client",
          newClient)
        newClient.asInstanceOf[HiveClientImpl]
    }

    val state = clientImpl.state
    SessionState.setCurrentSessionState(state)
    val user = UserGroupInformation.getCurrentUser.getShortUserName
    if (state.getAuthenticator.getUserName != user) {
      val hiveConf = state.getConf
      val newState = new SessionState(hiveConf, user)
      SessionState.start(newState)
      AuthzUtils.setFieldVal(clientImpl, "state", newState)
    }

    val authz = clientImpl.state.getAuthorizerV2
    clientImpl.withHiveState {
      if (authz != null) {
        try {
          authz.checkPrivileges(hiveOpType, inputObjs, outputObjs, context)
        } catch {
          case hae: HiveAccessControlException =>
            logger.error(
              s"""
                 |+===============================+
                 ||Spark SQL Authorization Failure|
                 ||-------------------------------|
                 ||${hae.getMessage}
                 ||-------------------------------|
                 ||Spark SQL Authorization Failure|
                 |+===============================+
               """.stripMargin)
            throw hae
          case e: Exception => throw e
        }
      } else {
        logger.warn("Authorizer V2 not configured. Skipping privilege checking")
      }
    }
  }
}
