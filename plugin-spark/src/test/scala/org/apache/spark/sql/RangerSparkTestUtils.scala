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

package org.apache.spark.sql

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.catalyst.optimizer.{RangerSparkMaskingExtension, RangerSparkRowFilterExtension}
import org.apache.spark.sql.execution.RangerSparkPlanOmitStrategy

object RangerSparkTestUtils {

  def injectRules(spark: SparkSession): Unit = {
    spark.extensions.injectOptimizerRule(RangerSparkRowFilterExtension)
    spark.extensions.injectOptimizerRule(RangerSparkMaskingExtension)
    spark.extensions.injectPlannerStrategy(RangerSparkPlanOmitStrategy)
  }

  def withUser[T](user: String)(f: => T): T = {
    val ugi = UserGroupInformation.createRemoteUser(user)
    ugi.doAs(new PrivilegedExceptionAction[T] {
      override def run(): T = f
    })
  }
}
