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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.plans.logical.{RangerSparkMasking, RangerSparkRowFilter}
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.FunSuite

class RangerSparkPlanOmitStrategyTest extends FunSuite {

  private val spark = TestHive.sparkSession

  test("ranger spark plan omit strategy") {
    val strategy = RangerSparkPlanOmitStrategy(spark)
    val df = spark.range(0, 5)
    val plan1 = df.queryExecution.optimizedPlan
    assert(strategy.apply(plan1) === Nil)
    val plan2 = RangerSparkRowFilter(plan1)
    assert(strategy.apply(plan2) === PlanLater(plan1) :: Nil)
    val plan3 = RangerSparkMasking(plan1)
    assert(strategy.apply(plan3) === PlanLater(plan1) :: Nil)
    val plan4 = RangerSparkMasking(plan2)
    assert(strategy.apply(plan4) === PlanLater(plan2) :: Nil)
    val plan5 = RangerSparkRowFilter(plan3)
    assert(strategy.apply(plan5) === PlanLater(plan3) :: Nil)
  }
}
