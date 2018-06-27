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

package org.apache.spark.sql.hive

import scala.util.{Failure, Success, Try}

private[hive] object AuthzUtils {

  def getFieldVal(o: Any, name: String): Any = {
    Try {
      val field = o.getClass.getDeclaredField(name)
      field.setAccessible(true)
      field.get(o)
    } match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }
  }

  def setFieldVal(o: Any, name: String, value: Any): Unit = {
    Try {
      val field = o.getClass.getDeclaredField(name)
      field.setAccessible(true)
      field.set(o, value.asInstanceOf[AnyRef])
    } match {
      case Failure(exception) => throw exception
      case _ =>
    }
  }
}
