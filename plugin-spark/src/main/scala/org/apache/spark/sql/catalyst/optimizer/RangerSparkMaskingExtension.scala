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
import org.apache.spark.sql.catalyst.catalog.{CatalogFunction, CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, ExprId, NamedExpression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateViewCommand, InsertIntoDataSourceDirCommand}
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand, LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveDirCommand, InsertIntoHiveTable}

import scala.collection.JavaConverters._
import scala.collection.mutable

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
    .foreach(spark.sessionState.catalog.registerFunction(_, true))

  private lazy val sparkPlugin = RangerSparkPlugin.build().getOrCreate()
  private lazy val sqlParser = spark.sessionState.sqlParser
  private lazy val analyzer = spark.sessionState.analyzer
  private lazy val rangerSparkOptimizer = new RangerSparkOptimizer(spark)

  /**
   * Collecting transformers from Ranger data masking policies, and mapping the to the
   * [[LogicalPlan]] output attributes.
   *
   * @param plan the original logical plan with a underlying catalog table
   * @param table the catalog table
   * @return a list of key-value pairs of original expression with its masking representation
   */
  private def collectTransformers(
      plan: LogicalPlan,
      table: CatalogTable,
      aliases: mutable.Map[Alias, ExprId]): Map[ExprId, NamedExpression] = {
    val auditHandler = new RangerSparkAuditHandler()
    val ugi = UserGroupInformation.getCurrentUser
    val userName = ugi.getShortUserName
    val groups = ugi.getGroupNames.toSet
    try {
      val identifier = table.identifier
      import SparkObjectType._

      val maskEnableResults = plan.output.map { expr =>
        val resource = RangerSparkResource(COLUMN, identifier.database, identifier.table, expr.name)
        val req = new RangerSparkAccessRequest(resource, userName, groups, COLUMN.toString,
          SparkAccessType.SELECT, sparkPlugin.getClusterName)
        (expr, sparkPlugin.evalDataMaskPolicies(req, auditHandler))
      }.filter(x => isMaskEnabled(x._2))

      val originMaskers = maskEnableResults.map { case (expr, result) =>
        if (StringUtils.equalsIgnoreCase(result.getMaskType, MASK_TYPE_NULL)) {
          val sql = s"SELECT NULL AS ${expr.name} FROM ${table.qualifiedName}"
          val plan = analyzer.execute(sqlParser.parsePlan(sql))
          (expr, plan)
        } else if (StringUtils.equalsIgnoreCase(result.getMaskType, MASK_TYPE_CUSTOM)) {
          val maskVal = result.getMaskedValue
          if (maskVal == null) {
            val sql = s"SELECT NULL AS ${expr.name} FROM ${table.qualifiedName}"
            val plan = analyzer.execute(sqlParser.parsePlan(sql))
            (expr, plan)
          } else {
            val sql = s"SELECT ${maskVal.replace("{col}", expr.name)} AS ${expr.name} FROM" +
              s" ${table.qualifiedName}"
            val plan = analyzer.execute(sqlParser.parsePlan(sql))
            (expr, plan)
          }
        } else if (result.getMaskTypeDef != null) {
          val transformer = result.getMaskTypeDef.getTransformer
          if (StringUtils.isNotEmpty(transformer)) {
            val trans = transformer.replace("{col}", expr.name)
            val sql = s"SELECT $trans AS ${expr.name} FROM ${table.qualifiedName}"
            val plan = analyzer.execute(sqlParser.parsePlan(sql))
            (expr, plan)
          } else {
            (expr, null)
          }
        } else {
          (expr, null)
        }
      }.filter(_._2 != null)

      val formedMaskers: Map[ExprId, Alias] =
        originMaskers.map { case (expr, p) => (expr, p.asInstanceOf[Project].projectList.head) }
          .map { case (expr, attr) =>
            val originalAlias = attr.asInstanceOf[Alias]
            val newChild = originalAlias.child mapChildren {
              case _: AttributeReference => expr
              case o => o
            }
            val newAlias = originalAlias.copy(child = newChild)(
              originalAlias.exprId, originalAlias.qualifier, originalAlias.explicitMetadata)
            (expr.exprId, newAlias)
          }.toMap

      val aliasedMaskers = new mutable.HashMap[ExprId, Alias]()
      for ((alias, id) <- aliases if formedMaskers.contains(id)) {
        val originalAlias = formedMaskers(id)
        val newChild = originalAlias.child mapChildren {
          case ar: AttributeReference =>
            ar.copy(name = alias.name)(alias.exprId, alias.qualifier)
          case o => o
        }
        val newAlias = originalAlias.copy(child = newChild, alias.name)(
          originalAlias.exprId, originalAlias.qualifier, originalAlias.explicitMetadata)
        aliasedMaskers.put(alias.exprId, newAlias)
      }
      formedMaskers ++ aliasedMaskers
    } catch {
      case e: Exception => throw e
    }
  }

  private def isMaskEnabled(result: RangerAccessResult): Boolean = {
    result != null && result.isMaskEnabled
  }

  private def hasCatalogTable(plan: LogicalPlan): Boolean = plan match {
    case _: HiveTableRelation => true
    case l: LogicalRelation if l.catalogTable.isDefined => true
    case _ => false
  }

  private def collectAllAliases(plan: LogicalPlan): mutable.HashMap[Alias, ExprId] = {
    val aliases = new mutable.HashMap[Alias, ExprId]()
    plan.transformAllExpressions {
      case a: Alias =>
        a.child match {
          case ne: NamedExpression =>
            aliases.put(a, ne.exprId)
          case _ =>
        }
        a
    }
    aliases
  }

  private def collectAllTransformers(
      plan: LogicalPlan, aliases: mutable.Map[Alias, ExprId]): Map[ExprId, NamedExpression] = {
    plan.collectLeaves().flatMap {
      case h: HiveTableRelation =>
        collectTransformers(h, h.tableMeta, aliases)
      case l: LogicalRelation if l.catalogTable.isDefined =>
        collectTransformers(l, l.catalogTable.get, aliases)
      case _ => Seq.empty
    }.toMap
  }

  private def doMasking(plan: LogicalPlan): LogicalPlan = plan match {
    case s: Subquery => s
    case m: RangerSparkMasking => m // escape the optimize iteration if already masked
    case fixed if fixed.find(_.isInstanceOf[RangerSparkMasking]).nonEmpty => fixed
    case _ =>
      val aliases = collectAllAliases(plan)
      val transformers = collectAllTransformers(plan, aliases)
      val newPlan =
        if (transformers.nonEmpty && plan.output.exists(o => transformers.get(o.exprId).nonEmpty)) {
          val newOutput = plan.output.map(attr => transformers.getOrElse(attr.exprId, attr))
          Project(newOutput, plan)
        } else {
          plan
        }

      val marked = newPlan transformUp {
        case p if hasCatalogTable(p) => RangerSparkMasking(p)
      }

      marked transformAllExpressions {
        case s: SubqueryExpression =>
          val Subquery(newPlan) =
            rangerSparkOptimizer.execute(Subquery(RangerSparkMasking(s.plan)))
          s.withNewPlan(newPlan)
      }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case c: Command => c match {
      case c: CreateDataSourceTableAsSelectCommand => c.copy(query = doMasking(c.query))
      case c: CreateHiveTableAsSelectCommand => c.copy(query = doMasking(c.query))
      case c: CreateViewCommand => c.copy(child = doMasking(c.child))
      case i: InsertIntoDataSourceCommand => i.copy(query = doMasking(i.query))
      case i: InsertIntoDataSourceDirCommand => i.copy(query = doMasking(i.query))
      case i: InsertIntoHadoopFsRelationCommand => i.copy(query = doMasking(i.query))
      case i: InsertIntoHiveDirCommand => i.copy(query = doMasking(i.query))
      case i: InsertIntoHiveTable => i.copy(query = doMasking(i.query))
      case s: SaveIntoDataSourceCommand => s.copy(query = doMasking(s.query))
      case cmd => cmd
    }
    case other => doMasking(other)
  }
}
