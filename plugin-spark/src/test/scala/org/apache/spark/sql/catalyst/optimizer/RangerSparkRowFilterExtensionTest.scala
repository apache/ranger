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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.FunSuite
import org.apache.spark.sql.RangerSparkTestUtils._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, RangerSparkRowFilter}

class RangerSparkRowFilterExtensionTest extends FunSuite {

  private val spark = TestHive.sparkSession

  test("ranger spark row filter extension") {
    val extension = RangerSparkRowFilterExtension(spark)
    val plan = spark.sql("select * from src").queryExecution.optimizedPlan
    println(plan)
    withUser("bob") {
      val newPlan = extension.apply(plan)
      assert(newPlan.isInstanceOf[RangerSparkRowFilter])
      val filters = newPlan.collect { case f: Filter => f }
      assert(filters.nonEmpty, "ranger row level filters should be applied automatically")
      println(newPlan)
    }
    withUser("alice") {
      val newPlan = extension.apply(plan)
      assert(newPlan.isInstanceOf[RangerSparkRowFilter])
      val filters = newPlan.collect { case f: Filter => f }
      assert(filters.isEmpty, "alice does not have implicit filters")
      println(newPlan)
    }
  }

}
