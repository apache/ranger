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

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.ranger.authorization.spark.authorizer._
import org.apache.ranger.plugin.model.RangerPolicy
import org.apache.ranger.plugin.policyengine.RangerAccessResult
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, RangerSparkMasking, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.AuthzUtils.getFieldVal
import org.apache.spark.sql.catalyst.catalog.{CatalogFunction, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.collection.JavaConverters._

/**
 * An Apache Spark's [[Optimizer]] extension for column data masking.
 */
case class RangerSparkMaskingExtension(spark: SparkSession) extends Rule[LogicalPlan] {
  import RangerPolicy._

  // register all built-in masking udfs
  Map("mask" -> "org.apache.hadoop.hive.ql.udf.generic.GenericUDFMask",
    "mask_first_n" -> "org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskFirstN",
    "mask_hash" -> "org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskHash",
    "mask_last_n" -> "org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskLastN",
    "mask_show_first_n" -> "org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskShowFirstN",
    "mask_show_last_n" -> "org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskShowLastN")
    .map(x => CatalogFunction(FunctionIdentifier(x._1), x._2, Seq.empty))
    .foreach(spark.sessionState.catalog.registerFunction(_, ignoreIfExists = true))

  private lazy val sparkPlugin = RangerSparkPlugin.build().getOrCreate()

  /**
   * Collecting transformers from Ranger data masking policies, and mapping the to the
   * [[LogicalPlan]] output attributes.
   *
   * @param plan the original logical plan with a underlying catalog table
   * @param table the catalog table
   * @return a list of key-value pairs of original expression with its masking representation
   */
  private def collectTransformers(
      plan: LogicalPlan, table: CatalogTable): Seq[(Attribute, NamedExpression)] = {
    val auditHandler = new RangerSparkAuditHandler()
    val ugi = UserGroupInformation.getCurrentUser
    val userName = ugi.getShortUserName
    val groups = ugi.getGroupNames.toSet.asJava
    try {
      val identifier = table.identifier
      import SparkObjectType._

      plan.output.map { expr =>
        val resource = RangerSparkResource(COLUMN, identifier.database, identifier.table, expr.name)
        val req = new RangerSparkAccessRequest(resource, userName, groups, COLUMN.toString,
          SparkAccessType.SELECT, null, null, sparkPlugin.getClusterName)
        (expr, sparkPlugin.evalDataMaskPolicies(req, auditHandler))
      }.filter(x => isMaskEnabled(x._2)).map { x =>
        val expr = x._1
        val result = x._2
        if (StringUtils.equalsIgnoreCase(result.getMaskType, MASK_TYPE_NULL)) {
          val sql = s"SELECT NULL AS ${expr.name} FROM ${table.qualifiedName}"
          (expr, spark.sessionState.sqlParser.parsePlan(sql))
        } else if (StringUtils.equalsIgnoreCase(result.getMaskType, MASK_TYPE_CUSTOM)) {
          val maskVal = result.getMaskedValue
          if (maskVal == null) {
            val sql = s"SELECT NULL AS ${expr.name} FROM ${table.qualifiedName}"
            (expr, spark.sessionState.sqlParser.parsePlan(sql))
          } else {
            val sql = s"SELECT ${maskVal.replace("{col}", expr.name)} AS ${expr.name} FROM" +
              s" ${table.qualifiedName}"
            (expr, spark.sessionState.sqlParser.parsePlan(sql))
          }
        } else if (result.getMaskTypeDef != null) {
          val transformer = result.getMaskTypeDef.getTransformer
          if (StringUtils.isNotEmpty(transformer)) {
            val trans = transformer.replace("{col}", expr.name)
            val sql = s"SELECT $trans AS ${expr.name} FROM ${table.qualifiedName}"
            (expr, spark.sessionState.sqlParser.parsePlan(sql))
          } else {
            (expr, null)
          }
        } else {
          (expr, null)
        }
      }.filter(_._2 != null).map { kv =>
        kv._2 match {
          case p: Project => (kv._1, p.projectList.head)
          case _ => (kv._1, kv._2.output.head)
        }
      }
    } catch {
      case e: Exception => throw e
    }
  }

  private def isMaskEnabled(result: RangerAccessResult): Boolean = {
    result != null && result.isMaskEnabled
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case m: RangerSparkMasking => m // escape the optimize iteration if already masked
    case fixed if fixed.find(_.isInstanceOf[RangerSparkMasking]).nonEmpty => fixed
    case _ =>
      val transformers = plan.collectLeaves().flatMap {
        case h if h.nodeName == "HiveTableRelation" =>
          val ct = getFieldVal(h, "tableMeta").asInstanceOf[CatalogTable]
          collectTransformers(h, ct)
        case m if m.nodeName == "MetastoreRelation" =>
          val ct = getFieldVal(m, "catalogTable").asInstanceOf[CatalogTable]
          collectTransformers(m, ct)
        case l: LogicalRelation if l.catalogTable.isDefined =>
          collectTransformers(l, l.catalogTable.get)
        case _ => Seq.empty
      }.toMap
      val newOutput = plan.output.map(attr => transformers.getOrElse(attr, attr))
      if (newOutput.isEmpty) {
        plan match {
          case Subquery(child) => Subquery(RangerSparkMasking(child))
          case _ => RangerSparkMasking(plan)
        }
      } else {
        val analyzed = spark.sessionState.analyzer.execute(Project(newOutput, plan))
        RangerSparkMasking(analyzed)
      }
  }
}
