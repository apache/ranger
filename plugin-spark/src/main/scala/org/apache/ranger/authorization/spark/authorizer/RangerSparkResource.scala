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

import org.apache.ranger.authorization.spark.authorizer.SparkObjectType.SparkObjectType
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl

class RangerSparkResource(
    objectType: SparkObjectType,
    databaseOrUrl: Option[String],
    tableOrUdf: String,
    column: String) extends RangerAccessResourceImpl {
  import SparkObjectType._
  import RangerSparkResource._

  def this(objectType: SparkObjectType, databaseOrUrl: Option[String], tableOrUdf: String) = {
    this(objectType, databaseOrUrl, tableOrUdf, null)
  }

  def this(objectType: SparkObjectType, databaseOrUrl: Option[String]) = {
    this(objectType, databaseOrUrl, null)
  }

  objectType match {
    case DATABASE => setValue(KEY_DATABASE, databaseOrUrl.getOrElse("*"))
    case FUNCTION =>
      setValue(KEY_DATABASE, databaseOrUrl.getOrElse(""))
      setValue(KEY_UDF, tableOrUdf)
    case COLUMN =>
      setValue(KEY_DATABASE, databaseOrUrl.getOrElse("*"))
      setValue(KEY_TABLE, tableOrUdf)
      setValue(KEY_COLUMN, column)
    case TABLE | VIEW =>
      setValue(KEY_DATABASE, databaseOrUrl.getOrElse("*"))
      setValue(KEY_TABLE, tableOrUdf)
    case URI => setValue(KEY_URL, databaseOrUrl.getOrElse("*"))
    case _ =>
  }

  def getObjectType: SparkObjectType = objectType

  def getDatabase: String = getValue(KEY_DATABASE).asInstanceOf[String]

  def getTable: String = getValue(KEY_TABLE).asInstanceOf[String]

  def getUdf: String = getValue(KEY_UDF).asInstanceOf[String]

  def getColumn: String = getValue(KEY_COLUMN).asInstanceOf[String]

  def getUrl: String = getValue(KEY_URL).asInstanceOf[String]

}

object RangerSparkResource {

  def apply(objectType: SparkObjectType, databaseOrUrl: Option[String], tableOrUdf: String,
      column: String): RangerSparkResource = {
    new RangerSparkResource(objectType, databaseOrUrl, tableOrUdf, column)
  }

  def apply(objectType: SparkObjectType, databaseOrUrl: Option[String],
            tableOrUdf: String): RangerSparkResource = {
    new RangerSparkResource(objectType, databaseOrUrl, tableOrUdf)
  }

  def apply(objectType: SparkObjectType, databaseOrUrl: Option[String]): RangerSparkResource = {
    new RangerSparkResource(objectType, databaseOrUrl)
  }

  private val KEY_DATABASE = "database"
  private val KEY_TABLE = "table"
  private val KEY_UDF = "udf"
  private val KEY_COLUMN = "column"
  private val KEY_URL = "url"
}
