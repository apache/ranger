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

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.ranger.authorization.spark.authorizer._
import org.apache.ranger.plugin.policyengine.RangerAccessResult
import org.apache.spark.sql.AuthzUtils.getFieldVal
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateViewCommand, InsertIntoDataSourceDirCommand}
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand, LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveDirCommand, InsertIntoHiveTable}

import scala.collection.JavaConverters._

/**
 * An Apache Spark's [[Optimizer]] extension for row level filtering.
 */
case class RangerSparkRowFilterExtension(spark: SparkSession) extends Rule[LogicalPlan] {
  private lazy val sparkPlugin = RangerSparkPlugin.build().getOrCreate()
  private lazy val rangerSparkOptimizer = new RangerSparkOptimizer(spark)

  /**
    * Transform a Relation to a parsed [[LogicalPlan]] with specified row filter expressions
    * @param plan the original [[LogicalPlan]]
    * @param table a Spark [[CatalogTable]] representation
    * @return A new Spark [[LogicalPlan]] with specified row filter expressions
    */
  private def applyingRowFilterExpr(plan: LogicalPlan, table: CatalogTable): LogicalPlan = {
    val auditHandler = new RangerSparkAuditHandler()
    try {
      val identifier = table.identifier
      val resource =
        RangerSparkResource(SparkObjectType.TABLE, identifier.database, identifier.table)
      val ugi = UserGroupInformation.getCurrentUser
      val request = new RangerSparkAccessRequest(resource, ugi.getShortUserName,
        ugi.getGroupNames.toSet, SparkObjectType.TABLE.toString, SparkAccessType.SELECT,
        sparkPlugin.getClusterName)
      val result = sparkPlugin.evalRowFilterPolicies(request, auditHandler)
      if (isRowFilterEnabled(result)) {
        val sql = s"select ${plan.output.map(_.name).mkString(",")} from ${table.qualifiedName}" +
          s" where ${result.getFilterExpr}"
        val parsed = spark.sessionState.sqlParser.parsePlan(sql)

        val parsedNew = parsed transform {
          case Filter(condition, child) if !child.fastEquals(plan) => Filter(condition, plan)
        }
        val analyzed = spark.sessionState.analyzer.execute(parsedNew)
        val optimized = analyzed transformAllExpressions {
          case s: SubqueryExpression =>
            val Subquery(newPlan) =
              rangerSparkOptimizer.execute(Subquery(RangerSparkRowFilter(s.plan)))
            s.withNewPlan(newPlan)
        }
        RangerSparkRowFilter(optimized)
      } else {
        RangerSparkRowFilter(plan)
      }
    } catch {
      case e: Exception => throw e
    }
  }

  private def isRowFilterEnabled(result: RangerAccessResult): Boolean = {
    result != null && result.isRowFilterEnabled && StringUtils.isNotEmpty(result.getFilterExpr)
  }

  private def doFiltering(plan: LogicalPlan): LogicalPlan = plan match {
    case rf: RangerSparkRowFilter => rf
    case fixed if fixed.find(_.isInstanceOf[RangerSparkRowFilter]).nonEmpty => fixed
    case _ =>
      val plansWithTables = plan.collectLeaves().map {
        case h if h.nodeName == "HiveTableRelation" =>
          (h, getFieldVal(h, "tableMeta").asInstanceOf[CatalogTable])
        case m if m.nodeName == "MetastoreRelation" =>
          (m, getFieldVal(m, "catalogTable").asInstanceOf[CatalogTable])
        case l: LogicalRelation if l.catalogTable.isDefined =>
          (l, l.catalogTable.get)
        case _ => null
      }.filter(_ != null).map(lt => (lt._1, applyingRowFilterExpr(lt._1, lt._2))).toMap

      plan transformUp {
        case p => plansWithTables.getOrElse(p, p)
      }
  }

  /**
   * Transform a spark logical plan to another plan with the row filer expressions
   * @param plan the original [[LogicalPlan]]
   * @return the logical plan with row filer expressions applied
   */
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case c: Command => c match {
      case c: CreateDataSourceTableAsSelectCommand => c.copy(query = doFiltering(c.query))
      case c: CreateHiveTableAsSelectCommand => c.copy(query = doFiltering(c.query))
      case c: CreateViewCommand => c.copy(child = doFiltering(c.child))
      case i: InsertIntoDataSourceCommand => i.copy(query = doFiltering(i.query))
      case i: InsertIntoDataSourceDirCommand => i.copy(query = doFiltering(i.query))
      case i: InsertIntoHadoopFsRelationCommand => i.copy(query = doFiltering(i.query))
      case i: InsertIntoHiveDirCommand => i.copy(query = doFiltering(i.query))
      case i: InsertIntoHiveTable => i.copy(query = doFiltering(i.query))
      case s: SaveIntoDataSourceCommand => s.copy(query = doFiltering(s.query))
      case cmd => cmd
    }
    case other => doFiltering(other)
  }
}
