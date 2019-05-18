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

import org.apache.ranger.authorization.spark.authorizer.SparkPrivilegeObjectType.SparkPrivilegeObjectType

import scala.collection.JavaConverters._
import org.apache.ranger.authorization.spark.authorizer.SparkPrivObjectActionType.SparkPrivObjectActionType

class SparkPrivilegeObject(
    private val typ: SparkPrivilegeObjectType,
    private val dbname: String,
    private val objectName: String,
    private val partKeys: Seq[String],
    private val columns: Seq[String],
    private val actionType: SparkPrivObjectActionType)
  extends Ordered[SparkPrivilegeObject] {

  override def compare(that: SparkPrivilegeObject): Int = {
    typ compareTo that.typ match {
      case 0 =>
        compare(dbname, that.dbname) match {
          case 0 =>
            compare(objectName, that.objectName) match {
              case 0 =>
                compare(partKeys, that.partKeys) match {
                  case 0 => compare(columns, that.columns)
                  case o => o
                }
              case o => o
            }
          case o => o
        }
      case o => o
    }
  }

  private def compare(o1: String, o2: String): Int = {
    if (o1 != null) {
      if (o2 != null) o1.compareTo(o2) else 1
    } else {
      if (o2 != null) -1 else 0
    }
  }

  private def compare(o1: Seq[String], o2: Seq[String]): Int = {
    if (o1 != null) {
      if (o2 != null) {
        for ((x, y) <- o1.zip(o2)) {
          val ret = compare(x, y)
          if (ret != 0) {
            return ret
          }
        }
        if (o1.size > o2.size) {
          1
        } else if (o1.size < o2.size) {
          -1
        } else {
          0
        }
      } else {
        1
      }
    } else {
      if (o2 != null) {
        -1
      } else {
        0
      }
    }
  }

  def this(typ: SparkPrivilegeObjectType, dbname: String, objectName: String,
           partKeys: Seq[String], columns: Seq[String]) =
    this(typ, dbname, objectName, partKeys, columns, SparkPrivObjectActionType.OTHER)

  def this(typ: SparkPrivilegeObjectType, dbname: String, objectName: String,
           actionType: SparkPrivObjectActionType) =
    this(typ, dbname, objectName, Nil, Nil, actionType)

  def this(typ: SparkPrivilegeObjectType, dbname: String, objectName: String) =
    this(typ, dbname, objectName, SparkPrivObjectActionType.OTHER)

  def getType: SparkPrivilegeObjectType = typ

  def getDbname: String = dbname

  def getObjectName: String = objectName

  def getActionType: SparkPrivObjectActionType = actionType

  def getPartKeys: Seq[String] = partKeys

  def getColumns: Seq[String] = columns

  override def toString: String = {
    val name = typ match {
      case SparkPrivilegeObjectType.DATABASE => dbname
      case SparkPrivilegeObjectType.TABLE_OR_VIEW =>
        getDbObjectName + (if (partKeys != null) partKeys.asJava.toString else "")
      case SparkPrivilegeObjectType.FUNCTION => getDbObjectName
      case _ => ""
    }

    val at = if (actionType != null) {
      actionType match {
        case SparkPrivObjectActionType.INSERT |
             SparkPrivObjectActionType.INSERT_OVERWRITE => ", action=" + actionType
        case _ => ""
      }
    } else {
      ""
    }
    "Object [type=" + typ + ", name=" + name + at + "]"
  }

  private def getDbObjectName: String = {
    (if (dbname == null) "" else dbname + ".") + objectName
  }
}
