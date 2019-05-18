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
import org.apache.spark.sql.RangerSparkTestUtils._
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{Project, RangerSparkMasking}
import org.scalatest.FunSuite

class RangerSparkMaskingExtensionTest extends FunSuite {

  private val spark = TestHive.sparkSession

  test("data masking for bob show last 4") {
    val extension = RangerSparkMaskingExtension(spark)
    val plan = spark.sql("select * from src").queryExecution.optimizedPlan
    println(plan)
    withUser("bob") {
      val newPlan = extension.apply(plan)
      assert(newPlan.isInstanceOf[Project])
      val project = newPlan.asInstanceOf[Project]
      val key = project.projectList.head
      assert(key.name === "key", "no affect on un masking attribute")
      val value = project.projectList.tail
      assert(value.head.name === "value", "attibute name should be unchanged")
      assert(value.head.asInstanceOf[Alias].child.sql ===
        "mask_show_last_n(`value`, 4, 'x', 'x', 'x', -1, '1')")
    }

    withUser("alice") {
      val newPlan = extension.apply(plan)
      assert(newPlan === RangerSparkMasking(plan))
    }
  }

}
