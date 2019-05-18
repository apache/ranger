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

import org.apache.ranger.authorization.spark.authorizer.SparkAccessControlException
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.FunSuite
import org.apache.spark.sql.RangerSparkTestUtils._
import org.apache.spark.sql.execution.{RangerShowDatabasesCommand, RangerShowTablesCommand}

class RangerSparkAuthorizerExtensionTest extends FunSuite {
  private val spark = TestHive.sparkSession
  val extension = RangerSparkAuthorizerExtension(spark)

  test("convert show tables command") {
    val df = spark.sql("show tables")
    val plan = df.queryExecution.optimizedPlan
    val newPlan = extension.apply(plan)
    assert(newPlan.isInstanceOf[RangerShowTablesCommand])
    assert(extension.apply(newPlan) === newPlan)
  }

  test("convert show databases command") {
    val df = spark.sql("show databases")
    val plan = df.queryExecution.optimizedPlan
    val newPlan = extension.apply(plan)
    assert(newPlan.isInstanceOf[RangerShowDatabasesCommand])
    assert(extension.apply(newPlan) === newPlan)
  }

  test("simple select") {
    val df = spark.sql("select * from src")
    val plan = df.queryExecution.optimizedPlan
    withUser("bob") {
      assert(extension.apply(plan) === plan, "bob has all privileges of table src")
    }
    withUser("alice") {
      val e = intercept[SparkAccessControlException](extension.apply(plan))
      assert(e.getMessage === "Permission denied: user [alice] does not have [SELECT] privilege" +
        " on [default/src/key,value]", "alice is not allow to access table src")
    }
    withUser("kent") {
      val e = intercept[SparkAccessControlException](extension.apply(plan))
      assert(e.getMessage === "Permission denied: user [kent] does not have [SELECT] privilege" +
        " on [default/src/key,value]", "kent can only access table src.key")
    }
  }

  test("projection select") {
    val df1 = spark.sql("select key from src")
    val df2 = spark.sql("select value from src")

    val plan1 = df1.queryExecution.optimizedPlan
    val plan2 = df2.queryExecution.optimizedPlan

    withUser("bob") {
      assert(extension.apply(plan1) === plan1, "bob has all privileges of table src")
      assert(extension.apply(plan2) === plan2, "bob has all privileges of table src")
    }
    withUser("alice") {
      val e = intercept[SparkAccessControlException](extension.apply(plan1))
      assert(e.getMessage === "Permission denied: user [alice] does not have [SELECT] privilege" +
        " on [default/src/key]", "alice is not allow to access table src")
    }
    withUser("kent") {
      assert(extension.apply(plan1) === plan1, "kent can only access table src.key")
      val e = intercept[SparkAccessControlException](extension.apply(plan2))
      assert(e.getMessage === "Permission denied: user [kent] does not have [SELECT] privilege" +
        " on [default/src/value]", "kent can only access table src.key")
    }
  }

  test("alter database set properties") {
    val df = spark.sql("ALTER DATABASE default SET DBPROPERTIES (hero='i am iron man')")
    val plan = df.queryExecution.optimizedPlan
    withUser("bob") {
      assert(extension.apply(plan) === plan)
    }
    withUser("alice") {
      val e = intercept[SparkAccessControlException](extension.apply(plan))
      assert(e.getMessage === "Permission denied: user [alice] does not have [ALTER] privilege" +
        " on [default]", "alice is not allow to set properties to default")
    }
  }
}
